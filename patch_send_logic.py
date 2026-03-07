import re

with open("app.py", "r", encoding="utf-8") as f:
    content = f.read()

# 1. Update send_batch Italian Footer
old_footer = '''                    # Duplicate prevention + unsubscribe footer
                    if lead_id:
                        token     = get_unsubscribe_token(lead_id)
                        unsub_url = f"{APP_BASE_URL}/unsubscribe/{token}"
                        body_send = body + f"\\n\\n---\\nPer non ricevere più email: {unsub_url}"'''

new_footer = '''                    # Duplicate prevention + unsubscribe footer
                    if lead_id:
                        from email_engine import add_unsubscribe_footer
                        token     = get_unsubscribe_token(lead_id)
                        body_send = add_unsubscribe_footer(body, token, APP_BASE_URL)'''
content = content.replace(old_footer, new_footer)

# 2. Update _do_send_pipeline
start_marker = 'def _do_send_pipeline(pipeline_id):'
end_marker = '        conn_final.close()'

start_idx = content.find(start_marker)
# Find the specific conn_final.close() that ends _do_send_pipeline
end_idx = content.find(end_marker, start_idx) + len(end_marker)

new_do_send = '''def _do_send_pipeline(pipeline_id):
    """
    Core sending logic. Uses an atomic DB claim to prevent double-sending
    even if this function is called from multiple threads simultaneously.
    """
    from database import db_session
    # ATOMIC CLAIM: only one thread can transition status from 'ready' → 'sending'
    with db_session() as conn:
        result = conn.execute(
            "UPDATE pipeline_runs SET status='sending' WHERE id=? AND status IN ('ready', 'error')",
            (pipeline_id,)
        )
        conn.commit()
        if result.rowcount == 0:
            return  # Another thread already claimed it, or it's not in a sendable state

        run = conn.execute("SELECT * FROM pipeline_runs WHERE id=?", (pipeline_id,)).fetchone()

    if not run or not run["results_json"]:
        return

    # Parse businesses (handling potential double-encoding)
    try:
        businesses = json.loads(run["results_json"])
        parsed = []
        for b in businesses:
            if isinstance(b, str):
                try:
                    parsed.append(json.loads(b))
                except Exception:
                    pass
            elif isinstance(b, dict):
                parsed.append(b)
        businesses = parsed
    except Exception:
        return

    qualified = [b for b in businesses if b.get("qualified") and b.get("email_subject")]
    if not qualified:
        return

    settings = get_settings()
    sent_count = run["sent"] or 0
    fail_count = run["failed"] or 0
    bounce_count = run["bounced"] or 0
    
    # ── Sending controls from DB settings ─────────────────
    delay_min        = int(settings.get("send_delay_min", 30))
    delay_max        = int(settings.get("send_delay_max", 60))
    micro_test_on    = settings.get("micro_test_enabled", "true").lower() == "true"
    micro_test_size  = int(settings.get("micro_test_size", 2))
    pause_on_bounce  = settings.get("pause_on_bounce", "true").lower() == "true"

    micro_test_done    = not micro_test_on
    micro_test_sent    = 0
    micro_test_bounced = 0

    def _abort_check():
        try:
            with db_session() as c:
                row = c.execute("SELECT status FROM pipeline_runs WHERE id=?", (pipeline_id,)).fetchone()
                return row and row["status"] in ("error", "stopped", "paused")
        except Exception:
            return False

    def _flush_counters():
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

            # Micro-test gate
            if not micro_test_done and micro_test_sent >= micro_test_size:
                micro_test_done = True
                if micro_test_bounced > 0:
                    print(f"[Pipeline {pipeline_id}] Micro-test FAILED ({micro_test_bounced} bounces in first {micro_test_size}). Pausing.")
                    with db_session() as c:
                        c.execute(
                            "UPDATE pipeline_runs SET status='error', results_json=? WHERE id=?",
                            (json.dumps({"error": f"Micro-test failed: {micro_test_bounced}/{micro_test_size} initial emails bounced."}), pipeline_id)
                        )
                        c.commit()
                    return
                else:
                    print(f"[Pipeline {pipeline_id}] Micro-test PASSED. Continuing full batch.")

            if biz.get("dispatch_status") in ("sent", "failed", "bounced", "skipped"):
                continue

            email = biz.get("email")
            subject = biz.get("email_subject")
            body = biz.get("email_body")

            if not email or not subject or not body or not is_good_email(email, biz.get("website", "")):
                fail_count += 1
                biz["dispatch_status"] = "skipped"
                continue

            try:
                lead_data = biz.copy()
                lead_data["email"] = email
                lead_data["email_source"] = biz.get("email_source", "pipeline")
                lead_data["google_rating"] = float(biz.get("google_rating", 0.0) or 0.0)
                lead_data["review_count"] = int(biz.get("review_count", 0) or 0)
                lead_id = add_lead(lead_data)
            except Exception:
                try:
                    with db_session() as conn_lookup:
                        row = conn_lookup.execute("SELECT id FROM leads WHERE email=?", (email,)).fetchone()
                        lead_id = row["id"] if row else None
                except Exception:
                    lead_id = None

            if lead_id:
                from database import get_unsubscribe_token, is_lead_unsubscribed, is_lead_bounced
                if is_lead_unsubscribed(lead_id) or is_lead_bounced(lead_id):
                    biz["dispatch_status"] = "skipped"
                    continue

                from email_engine import add_unsubscribe_footer
                token = get_unsubscribe_token(lead_id)
                body_send = add_unsubscribe_footer(body, token, APP_BASE_URL)

                with db_session() as conn_dup:
                    past = conn_dup.execute(
                        "SELECT id FROM email_log WHERE lead_id=? AND status IN ('sent','bounced')",
                        (lead_id,)
                    ).fetchone()
                if past:
                    biz["dispatch_status"] = "skipped"
                    continue
            else:
                body_send = body

            send_ts = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
            success, error_msg, is_bounce = send_email(email, subject, body_send, settings)
            dispatch_status = "sent" if success else ("bounced" if is_bounce else "failed")

            if is_bounce and lead_id:
                mark_bounced(lead_id)

            try:
                log_email({
                    "lead_id": lead_id or 0,
                    "campaign_id": 0,
                    "sequence_step": 1,
                    "business_type": biz.get("business_type", "other"),
                    "subject": subject,
                    "body": body_send,
                    "status": dispatch_status,
                    "error_message": f"[{send_ts}] {error_msg}" if error_msg else None,
                    "tier": biz.get("tier", 3),
                    "qualification_score": biz.get("score", 0),
                    "city": biz.get("city", ""),
                    "country": "",
                })
            except Exception:
                pass

            biz["dispatch_status"] = dispatch_status

            if success:
                sent_count += 1
                micro_test_sent += 1
                try:
                    add_lead_to_sequence(lead_id)
                except Exception:
                    pass
            elif is_bounce:
                bounce_count += 1
                micro_test_sent += 1
                micro_test_bounced += 1
                if pause_on_bounce and micro_test_done:
                    print(f"[Pipeline {pipeline_id}] Bounce for {email} post-micro-test. Pausing.")
                    with db_session() as c:
                        c.execute("UPDATE pipeline_runs SET status='error' WHERE id=?", (pipeline_id,))
                        c.commit()
                    _flush_counters()
                    return
            else:
                fail_count += 1

            _flush_counters()
            delay = random.randint(delay_min, delay_max)
            print(f"[_do_send_pipeline {pipeline_id}] [{dispatch_status.upper()}] {email} — waiting {delay}s...")
            time.sleep(delay)

    except Exception:
        import traceback
        traceback.print_exc()
    finally:
        with db_session() as conn_final:
            curr = conn_final.execute("SELECT status FROM pipeline_runs WHERE id=?", (pipeline_id,)).fetchone()
            if curr and curr["status"] not in ("error", "stopped", "paused"):
                conn_final.execute(
                    "UPDATE pipeline_runs SET status='done', sent=?, failed=?, bounced=? WHERE id=?",
                    (sent_count, fail_count, bounce_count, pipeline_id)
                )
            conn_final.commit()'''

content = content[:start_idx] + new_do_send + content[end_idx:]

with open("app.py", "w", encoding="utf-8") as f:
    f.write(content)

print("Patch applied successfully.")
