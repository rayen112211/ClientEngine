"""
One-time patch to replace the old send_batch function in app.py with the
bulletproof version that includes:
  - Micro-test gate (2 emails first, abort if any bounce)
  - Configurable 30-60s delay between sends from DB settings
  - Pause-on-bounce: auto-stops pipeline on post-micro-test bounce
  - Safe db_session usage throughout
  - Per-email delivery log with timestamp
"""

NEW_SEND_BATCH = '''    def send_batch(initial_sent, initial_failed, initial_bounced):
        settings = get_settings()
        sent_count      = initial_sent
        fail_count      = initial_failed
        bounce_count    = initial_bounced

        # ── Sending controls from DB settings ─────────────────
        delay_min        = int(settings.get("send_delay_min", 30))
        delay_max        = int(settings.get("send_delay_max", 60))
        micro_test_on    = settings.get("micro_test_enabled", "true").lower() == "true"
        micro_test_size  = int(settings.get("micro_test_size", 2))
        pause_on_bounce  = settings.get("pause_on_bounce", "true").lower() == "true"

        micro_test_done    = False
        micro_test_sent    = 0
        micro_test_bounced = 0

        def _abort_check():
            try:
                from database import db_session
                with db_session() as c:
                    row = c.execute("SELECT status FROM pipeline_runs WHERE id=?", (pipeline_id,)).fetchone()
                    return row and row["status"] == "error"
            except Exception:
                return False

        def _flush_counters():
            from database import db_session
            with db_session() as c:
                c.execute(
                    "UPDATE pipeline_runs SET sent=?, failed=?, bounced=?, results_json=? WHERE id=?",
                    (sent_count, fail_count, bounce_count,
                     json.dumps(businesses, ensure_ascii=False), pipeline_id)
                )
                c.commit()

        try:
            for i, biz in enumerate(qualified):
                # Abort check
                if _abort_check():
                    print(f"[Pipeline {pipeline_id}] Aborted by user at lead {i+1}.")
                    break

                # Micro-test gate: after first N emails, check bounce rate before continuing
                if micro_test_on and not micro_test_done and micro_test_sent >= micro_test_size:
                    micro_test_done = True
                    if micro_test_bounced > 0:
                        print(f"[Pipeline {pipeline_id}] Micro-test FAILED ({micro_test_bounced} bounces in first {micro_test_size}). Pausing.")
                        from database import db_session
                        with db_session() as c:
                            c.execute(
                                "UPDATE pipeline_runs SET status=\'error\', results_json=? WHERE id=?",
                                (json.dumps({"error": f"Micro-test failed: {micro_test_bounced}/{micro_test_size} initial emails bounced. Check SMTP config."}), pipeline_id)
                            )
                            c.commit()
                        return
                    else:
                        print(f"[Pipeline {pipeline_id}] Micro-test PASSED. Continuing full batch.")

                try:
                    # Skip already processed
                    if biz.get("dispatch_status") in ("sent", "failed", "bounced", "skipped"):
                        continue

                    email   = biz.get("email")
                    subject = biz.get("email_subject")
                    body    = biz.get("email_body")

                    if not email or not subject or not body:
                        fail_count += 1
                        continue

                    if not is_good_email(email, biz.get("website", "")):
                        fail_count += 1
                        biz["dispatch_status"] = "skipped"
                        continue

                    # Add / find lead
                    try:
                        lead_data = biz.copy()
                        lead_data["email"]         = email
                        lead_data["email_source"]  = biz.get("email_source", "pipeline")
                        lead_data["google_rating"] = float(biz.get("google_rating", 0.0) or 0.0)
                        lead_data["review_count"]  = int(biz.get("review_count", 0) or 0)
                        lead_id = add_lead(lead_data)
                    except Exception as lead_e:
                        try:
                            from database import db_session
                            with db_session() as c:
                                row = c.execute("SELECT id FROM leads WHERE email=?", (email,)).fetchone()
                                lead_id = row["id"] if row else None
                        except Exception:
                            lead_id = None

                    # Duplicate prevention + unsubscribe footer
                    if lead_id:
                        token     = get_unsubscribe_token(lead_id)
                        unsub_url = f"{APP_BASE_URL}/unsubscribe/{token}"
                        body_send = body + f"\\n\\n---\\nPer non ricevere più email: {unsub_url}"
                        try:
                            from database import db_session
                            with db_session() as c:
                                past = c.execute(
                                    "SELECT id FROM email_log WHERE lead_id=? AND status IN (\'sent\',\'bounced\')",
                                    (lead_id,)
                                ).fetchone()
                        except Exception:
                            past = None
                        if past:
                            biz["dispatch_status"] = "skipped"
                            continue
                    else:
                        body_send = body

                    # SEND
                    send_ts = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
                    success, error_msg, is_bounce = send_email(email, subject, body_send, settings)
                    dispatch_status = "sent" if success else ("bounced" if is_bounce else "failed")

                    if is_bounce and lead_id:
                        mark_bounced(lead_id)

                    # Structured delivery log
                    try:
                        log_email({
                            "lead_id":            lead_id or 0,
                            "campaign_id":        0,
                            "sequence_step":      1,
                            "business_type":      biz.get("business_type", "other"),
                            "subject":            subject,
                            "body":               body_send,
                            "status":             dispatch_status,
                            "error_message":      f"[{send_ts}] {error_msg}" if error_msg else None,
                            "tier":               biz.get("tier", 3),
                            "qualification_score": biz.get("score", 0),
                            "city":               biz.get("city", ""),
                            "country":            "",
                        })
                    except Exception:
                        pass

                    biz["dispatch_status"] = dispatch_status

                    if success:
                        sent_count += 1
                        micro_test_sent += 1
                        try:
                            add_lead_to_sequence(lead_id)
                        except Exception as seq_e:
                            print(f"[seq] {seq_e}")
                    elif is_bounce:
                        bounce_count += 1
                        micro_test_sent += 1
                        micro_test_bounced += 1
                        # Pause-on-bounce: stop pipeline after micro-test phase
                        if pause_on_bounce and micro_test_done:
                            print(f"[Pipeline {pipeline_id}] Bounce for {email} post-micro-test. Pausing.")
                            from database import db_session
                            with db_session() as c:
                                c.execute("UPDATE pipeline_runs SET status=\'error\' WHERE id=?", (pipeline_id,))
                                c.commit()
                            _flush_counters()
                            return
                    else:
                        fail_count += 1

                    _flush_counters()

                    # Rate limiting
                    delay = random.randint(delay_min, delay_max)
                    print(f"[Pipeline {pipeline_id}] [{dispatch_status.upper()}] {email} — waiting {delay}s...")
                    time.sleep(delay)

                except Exception as inner_e:
                    fail_count += 1
                    err_msg = f"Crash processing {email}: {inner_e}"
                    print(err_msg)
                    biz["dispatch_status"] = "failed"
                    try:
                        with open("data/send_errors.log", "a", encoding="utf-8") as lf:
                            lf.write(f"[{datetime.utcnow().isoformat()}] Pipeline {pipeline_id}: {err_msg}\\n")
                            import traceback
                            traceback.print_exc(file=lf)
                    except:
                        pass
                    continue
        except Exception as e:
            traceback.print_exc()
        finally:
            from database import db_session
            with db_session() as c:
                row = c.execute("SELECT status FROM pipeline_runs WHERE id=?", (pipeline_id,)).fetchone()
                final_status = "error" if (row and row["status"] == "error") else "sent"
                c.execute(
                    "UPDATE pipeline_runs SET status=?, sent=?, failed=?, bounced=?, results_json=? WHERE id=?",
                    (final_status, sent_count, fail_count, bounce_count,
                     json.dumps(businesses, ensure_ascii=False), pipeline_id)
                )
                c.commit()
            global _send_all_running
            _send_all_running = False
'''

import re

with open('app.py', 'r', encoding='utf-8') as f:
    content = f.read()

# Find the send_batch function boundaries
start_marker = '    def send_batch(initial_sent, initial_failed, initial_bounced):'
end_marker   = '    initial_sent = run_dict.get'

si = content.find(start_marker)
ei = content.find(end_marker)

if si == -1 or ei == -1:
    print(f'ERROR: Markers not found. si={si}, ei={ei}')
    exit(1)

old_block = content[si:ei]
new_content = content[:si] + NEW_SEND_BATCH + '\n' + content[ei:]

with open('app.py', 'w', encoding='utf-8') as f:
    f.write(new_content)

print(f'OK — replaced {len(old_block)} chars with {len(NEW_SEND_BATCH)} chars')
