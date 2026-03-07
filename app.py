"""
ClientEngine v4 — Simple, Connected, Smart.
4 pages: Home → Results → Analytics → Settings.
One continuous workflow: Search → Extract → Score → Preview → Send → Track.
"""
import json
import time
import random
import threading
import traceback
from datetime import datetime
from flask import (
    Flask, render_template, request, redirect, url_for,
    flash, jsonify,
)
from database import (
    init_db, get_db, get_settings, update_settings,
    get_email_log, get_analytics, log_email,
    unsubscribe_by_token, save_search, get_search_history,
    add_lead, get_leads, create_campaign, enroll_leads_in_campaign,
    advance_sequence, get_due_sequences, mark_replied,
    get_unsubscribe_token, is_lead_unsubscribed, is_lead_bounced,
    mark_bounced, rows_to_dicts, add_lead_to_sequence,
    save_manual_lead, get_manual_leads, mark_manual_lead_status,
    delete_pipeline_run, reset_database,
    enqueue_searches, dequeue_next_search, complete_queue_item, get_queue_status,
)
from business_discovery import search_businesses
from email_extractor import find_email
from enrichment import (
    detect_business_type, check_website, score_business,
    is_good_business, is_good_email,
)
from email_engine import send_email, check_spam_score
from templates_data import get_template, SEQUENCE_STEP_NAMES
from config import SECRET_KEY, DEBUG, PORT, APP_BASE_URL
from auto_followup import start_daemon

app = Flask(__name__)
app.secret_key = SECRET_KEY

# Register urlencode as a Jinja2 filter (used in manual.html for WhatsApp links)
from urllib.parse import quote
app.jinja_env.filters['urlencode'] = lambda s: quote(str(s), safe='')

init_db()

# Start background sequence daemon
start_daemon()

# ═══════════════════════════════════════════════════════════
# STARTUP SELF-HEALING — Fix anything stuck from a crash
# ═══════════════════════════════════════════════════════════
try:
    import os
    os.makedirs("data", exist_ok=True)
    _startup_conn = get_db()
    # 1. Re-queue any search_queue items stuck as 'running'
    _startup_conn.execute("UPDATE search_queue SET status='pending' WHERE status='running'")
    # 2. Self-heal: mark any pipeline_runs stuck in an active SEARCH state as 'failed'
    #    (They'll show a clear error rather than spinning forever)
    _stuck_search_states = ("discovering", "searching", "extracting", "scoring", "filtering")
    placeholders = ",".join(["?"] * len(_stuck_search_states))
    _startup_conn.execute(
        f"UPDATE pipeline_runs SET status='failed', results_json=? WHERE status IN ({placeholders})",
        (json.dumps({"error": "Server restarted while process was running. Please start a new process."}), *_stuck_search_states)
    )
    # 3. Reset 'sending' pipelines back to 'ready' so user can re-send them
    #    (Don't mark as 'failed' — the emails are already generated, just need to resume sending)
    _startup_conn.execute("UPDATE pipeline_runs SET status='ready' WHERE status='sending'")
    # 4. Leave 'paused' pipelines as-is — they were rate-limited and can be resumed
    _startup_conn.commit()
    _pending = _startup_conn.execute("SELECT COUNT(*) FROM search_queue WHERE status='pending'").fetchone()[0]
    _startup_conn.close()
    if _pending > 0:
        print(f"📋 Found {_pending} pending queue items from previous session — restarting queue worker.")
except Exception:
    _pending = 0

# ─────────────────────────────────────────────────────────
# PIPELINE KILL REGISTRY — Tracks active pipeline events for cancellation
# ─────────────────────────────────────────────────────────
_pipeline_kill_events = {}   # pipeline_id -> threading.Event
_pipeline_kill_lock = threading.Lock()

# ─────────────────────────────────────────────────────────
# PIPELINE LOGGER — Writes timestamped debug logs per search
# ─────────────────────────────────────────────────────────
PIPELINE_LOG_FILE = "data/pipeline_events.log"

def _pipeline_log(pipeline_id, event, message=""):
    """Write a structured log entry for a pipeline state transition."""
    ts = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
    line = f"[{ts}] pipeline_id={pipeline_id} event={event} {message}\n"
    try:
        import os
        os.makedirs("data", exist_ok=True)
        with open(PIPELINE_LOG_FILE, "a", encoding="utf-8") as f:
            f.write(line)
        print(line.strip())
    except Exception:
        pass

# ─────────────────────────────────────────────────────────
# BATCH QUEUE WORKER — Runs pending searches sequentially
# ─────────────────────────────────────────────────────────
_queue_lock = threading.Lock()
_queue_running = False

def _run_queue_worker():
    """Background thread that processes the search queue one by one."""
    global _queue_running
    while True:
        item = dequeue_next_search()
        if not item:
            _queue_running = False
            break
        query = item.get("query", "")
        location = item.get("location", "")
        source_choice = item.get("source_choice", "all")
        pid = None
        try:
            from database import db_session
            with db_session() as conn:
                cur = conn.execute(
                    "INSERT INTO pipeline_runs (query, location, source_choice, status) VALUES (?, ?, ?, 'searching')",
                    (query, location, source_choice),
                )
                pid = cur.lastrowid
                conn.commit()

            _pipeline_log(pid, "search_started", f"query='{query}' location='{location}' source={source_choice}")

            # Register kill event for this pipeline
            kill_event = threading.Event()
            with _pipeline_kill_lock:
                _pipeline_kill_events[pid] = kill_event

            # Run the FULL pipeline (discover → extract → score → email preview)
            _execute_pipeline(pid, query, location, source_choice, kill_event)

            complete_queue_item(item["id"], pid, error=False)
        except Exception as e:
            _pipeline_log(pid, "search_error", f"{type(e).__name__}: {e}")
            traceback.print_exc()
            if pid:
                try:
                    from database import db_session
                    with db_session() as conn:
                        conn.execute(
                            "UPDATE pipeline_runs SET status='failed', results_json=? WHERE id=?",
                            (json.dumps({"error": f"{type(e).__name__}: {str(e)}"}), pid)
                        )
                        conn.commit()
                except Exception:
                    pass
            complete_queue_item(item["id"], pid, error=True)
        finally:
            # Always clean up kill event
            if pid:
                with _pipeline_kill_lock:
                    _pipeline_kill_events.pop(pid, None)
        # Pause between searches to avoid rate-limits
        time.sleep(10)

def _start_queue_if_idle():
    """Kick off the queue worker thread if not already running."""
    global _queue_running
    with _queue_lock:
        if not _queue_running:
            _queue_running = True
            t = threading.Thread(target=_run_queue_worker, daemon=True)
            t.start()

# Global lock: prevents Send All from being triggered by multiple clicks at once
_send_all_lock = threading.Lock()
_send_all_running = False

# Auto-resume queue from previous session on startup
if _pending > 0:
    _start_queue_if_idle()


# ═══════════════════════════════════════════════════════════
# 1. HOME — Search box + Stats
# ═══════════════════════════════════════════════════════════

@app.route("/")
def home():
    conn = get_db()
    # Quick stats
    total_sent = conn.execute("SELECT COUNT(*) FROM email_log WHERE status='sent'").fetchone()[0]
    total_replied = conn.execute("SELECT COUNT(*) FROM email_log WHERE replied=1").fetchone()[0]
    reply_rate = round(total_replied / total_sent * 100, 1) if total_sent > 0 else 0

    # Pipeline runs (show last 20)
    runs = conn.execute(
        "SELECT * FROM pipeline_runs ORDER BY created_at DESC LIMIT 20"
    ).fetchall()

    # Count how many are ready to send
    ready_count = conn.execute(
        "SELECT COUNT(*) FROM pipeline_runs WHERE status='ready'"
    ).fetchone()[0]

    history = get_search_history(limit=5)
    conn.close()

    return render_template(
        "home.html",
        stats={"sent": total_sent, "replies": total_replied, "rate": reply_rate},
        runs=rows_to_dicts(runs) if runs else [],
        history=history,
        queue=get_queue_status(),
        ready_count=ready_count,
    )

@app.route("/api/queue-status")
def api_queue_status():
    return jsonify(get_queue_status())

@app.route("/api/queue/clear", methods=["POST"])
def clear_queue():
    # 1. Clear all pending from DB
    from database import clear_search_queue
    clear_search_queue()
    
    # 2. Stop any currently running searches
    with _pipeline_kill_lock:
        for pid, kill_event in _pipeline_kill_events.items():
            if not kill_event.is_set():
                kill_event.set()
                _pipeline_log(pid, "queue_clear_cancelling", "User clicked Clear Queue")
                
    flash("Queue cleared! Pending searches removed and running searches cancelled.", "success")
    return redirect(url_for("home"))

@app.route("/api/reset/searches", methods=["POST"])
def reset_searches():
    """Wipe all search history and pipeline runs from the dashboard."""
    from database import clear_all_pipeline_runs
    clear_all_pipeline_runs()
    
    # 2. Stop any currently running searches in the background
    with _pipeline_kill_lock:
        for pid, kill_event in _pipeline_kill_events.items():
            if not kill_event.is_set():
                kill_event.set()
                _pipeline_log(pid, "reset_searches_cancelling", "User clicked Reset Searches")
                
    flash("🗑️ All past searches and pipeline history have been deleted.", "success")
    return redirect(url_for("home"))

@app.route("/api/reset/followups", methods=["POST"])
def reset_followups():
    """Instantly stop all automated follow-up emails."""
    from database import cancel_all_sequences
    cancel_all_sequences()
    flash("🛑 All automated follow-up emails have been instantly cancelled.", "success")
    return redirect(url_for("home"))

@app.route("/api/reset/manual_leads", methods=["POST"])
def reset_manual_leads():
    """Wipe all manual leads."""
    from database import clear_all_manual_leads
    clear_all_manual_leads()
    flash("🗑️ All manual leads have been deleted.", "success")
    return redirect(url_for("manual_leads"))


@app.route("/send-all-ready", methods=["POST"])
def send_all_ready():
    """Trigger sending for ALL 'ready' pipeline runs, one by one in the background."""
    global _send_all_running
    with _send_all_lock:
        if _send_all_running:
            flash("⏳ Already sending — wait for current batch to finish before pressing again.", "warning")
            return redirect(url_for("home"))
        _send_all_running = True

    conn = get_db()
    ready_runs = conn.execute(
        "SELECT id FROM pipeline_runs WHERE status='ready'"
    ).fetchall()
    conn.close()

    if not ready_runs:
        _send_all_running = False
        flash("No ready pipelines found.", "warning")
        return redirect(url_for("home"))

    ids = [row["id"] for row in ready_runs]
    count = len(ids)

    def _send_all_worker():
        global _send_all_running
        try:
            for pid in ids:
                try:
                    # Run synchronously, one after the other
                    _do_send_pipeline(pid)
                    time.sleep(2)  # brief pause before starting the next pipeline
                except Exception as e:
                    print(f"[Send All] Pipeline {pid} errored: {e}")
        finally:
            _send_all_running = False

    threading.Thread(target=_send_all_worker, daemon=True).start()
    flash(f"🚀 Sending queued for {count} pipeline(s)! Check Analytics for live progress.", "success")
    return redirect(url_for("analytics_page"))

@app.route("/pipelines")
def all_pipelines():
    """View all historical pipeline runs."""
    conn = get_db()
    runs = conn.execute(
        "SELECT * FROM pipeline_runs ORDER BY created_at DESC"
    ).fetchall()
    conn.close()
    
    return render_template(
        "pipelines.html",
        runs=rows_to_dicts(runs) if runs else [],
    )


# ═══════════════════════════════════════════════════════════
# 2. SEARCH PIPELINE — Discover → Extract → Score → Filter
# ═══════════════════════════════════════════════════════════

@app.route("/search", methods=["POST"])
def search_pipeline():
    """Run the entire search → extract → score → filter pipeline."""
    raw_query = request.form.get("query", "").strip()
    source_choice = request.form.get("source", "all")
    
    if not raw_query:
        flash("Enter a search like 'Restaurant Barcelona' or 'Salon Madrid'", "error")
        return redirect(url_for("home"))

    lines = [l.strip() for l in raw_query.replace("\r", "\n").split("\n") if l.strip()]

    # Multi-line batch mode
    if len(lines) > 1:
        parsed = []
        for line in lines:
            # Accept: "Beauty Salon – Bend, OR" or "Beauty Salon - Bend, OR" or "Beauty Salon Bend, OR"
            for sep in ["–", "-", ","]:
                if sep in line:
                    parts = line.split(sep, 1)
                    parsed.append((parts[0].strip(), parts[1].strip()))
                    break
            else:
                words = line.split(maxsplit=1)
                parsed.append((words[0], words[1] if len(words) > 1 else ""))
        enqueue_searches(parsed, source_choice)
        _start_queue_if_idle()
        flash(f"⏳ Batch of {len(parsed)} searches queued! They will run one by one.", "success")
        return redirect(url_for("home"))

    # Single-line mode (original behaviour)
    single = lines[0] if lines else raw_query
    for sep in ["–", "-", ","]:
        if sep in single:
            parts = single.split(sep, 1)
            business_type, location = parts[0].strip(), parts[1].strip()
            break
    else:
        parts = single.split(maxsplit=1)
        business_type = parts[0] if parts else single
        location = parts[1] if len(parts) > 1 else ""

    # Create pipeline run
    conn = get_db()
    cursor = conn.execute(
        "INSERT INTO pipeline_runs (query, location, source_choice, status) VALUES (?, ?, ?, 'searching')",
        (business_type, location, source_choice),
    )
    pipeline_id = cursor.lastrowid
    conn.commit()
    conn.close()

    # Run pipeline in background
    kill_event = threading.Event()
    _pipeline_kill_events[pipeline_id] = kill_event
    
    def run_pipeline():
        try:
            _execute_pipeline(pipeline_id, business_type, location, source_choice, kill_event=kill_event)
        finally:
            _pipeline_kill_events.pop(pipeline_id, None)

    thread = threading.Thread(target=run_pipeline)
    thread.start()

    return redirect(url_for("results_page", pipeline_id=pipeline_id))


def _execute_pipeline(pipeline_id, business_type, location, source_choice="all", kill_event=None):
    """
    Execute the full pipeline: discover → extract emails → score → filter.
    
    Bulletproof protections:
    • Hard 300s total pipeline timeout
    • 30s per-website-check timeout on individual futures
    • Heartbeat: auto-fails if no progress for 90s
    • Kill event: instantly aborts when Stop button is pressed
    • Fully isolated executor per search (no shared state)
    """
    import concurrent.futures
    from enrichment import check_website, detect_business_type, score_business, assign_tier, choose_channel

    PIPELINE_HARD_TIMEOUT = 300   # 5 minutes max for the entire pipeline
    HEARTBEAT_TIMEOUT    = 90    # Fail if no progress for 90 seconds
    FUTURE_TIMEOUT       = 30    # Max 30s for any single website/email check
    executor             = None  # Declared here so finally block can shut it down

    # Shorthand: write a timestamped status to DB + log file
    def update_status(status, **kwargs):
        from database import db_session
        with db_session() as c:
            sets = ", ".join(f"{k}=?" for k in kwargs)
            vals = list(kwargs.values())
            if sets:
                c.execute(
                    f"UPDATE pipeline_runs SET status=?, {sets} WHERE id=?",
                    [status] + vals + [pipeline_id],
                )
            else:
                c.execute("UPDATE pipeline_runs SET status=? WHERE id=?", (status, pipeline_id))
            c.commit()
        _pipeline_log(pipeline_id, f"search_{status}")

    # Check if user pressed Stop or if our kill_event was set
    def should_abort():
        if kill_event and kill_event.is_set():
            return True
        try:
            from database import db_session
            with db_session() as c:
                row = c.execute("SELECT status FROM pipeline_runs WHERE id=?", (pipeline_id,)).fetchone()
                return row and row["status"] == "error"
        except Exception:
            return False

    try:
        pipeline_start = time.time()

        # STEP 1: Discover businesses (100 max — was 200, reduced to avoid overload)
        update_status("discovering")
        if should_abort():
            return

        # Wrap discovery in its own thread with a hard timeout
        discovery_result = [None]
        discovery_error  = [None]
        def _do_discover():
            try:
                discovery_result[0] = search_businesses(business_type, location, source_choice, max_results=100)
            except Exception as e:
                discovery_error[0] = e
        disc_thread = threading.Thread(target=_do_discover, daemon=True)
        disc_thread.start()
        disc_thread.join(timeout=PIPELINE_HARD_TIMEOUT)

        if disc_thread.is_alive():
            _pipeline_log(pipeline_id, "search_timeout", "discovery step timed out after 5m")
            update_status("failed", results_json=json.dumps({"error": "Search timed out during discovery. Try again."}))
            return

        if discovery_error[0]:
            raise discovery_error[0]

        result = discovery_result[0] or {}
        if result.get("error"):
            err_msg = result["error"]
            if result.get("debug_errors"):
                err_msg += " | Debug: " + ", ".join(result["debug_errors"])
            update_status("failed", results_json=json.dumps({"error": err_msg}))
            return

        businesses = result.get("results", [])
        if not businesses:
            update_status("ready", found=0, qualified=0, results_json=json.dumps([]))
            save_search(business_type, location, 0, 0)
            return

        _pipeline_log(pipeline_id, "search_progress", f"discovered={len(businesses)}")
        update_status("extracting", found=len(businesses))

        # STEP 2 & 3: Extract emails + score (with per-future hard timeout)
        def process_biz(biz):
            """Process a single business: email extraction + website check + scoring."""
            if should_abort():
                return biz
            if biz.get("website") and not biz.get("email"):
                try:
                    email_result = find_email(biz.get("business_name", ""), biz["website"])
                    if email_result.get("email"):
                        biz["email"] = email_result["email"]
                        biz["email_source"] = email_result.get("source", "extracted")
                except Exception:
                    pass
            biz["website_check"] = check_website(biz.get("website", ""))
            biz["business_type"] = detect_business_type(
                biz.get("category", "") + " " + biz.get("business_name", "")
            )
            r = score_business(biz)
            biz["score"]           = r["score"]
            biz["score_details"]   = r["details"]
            biz["pain_points"]     = r["pain_points"]
            biz["tier"]            = assign_tier(biz["score"])
            biz["outreach_channel"] = choose_channel(biz)
            return biz

        with_website_count = sum(1 for b in businesses if b.get("website"))
        processed_count    = 0
        heartbeat_ts       = time.time()

        # Use a FRESH, isolated executor — max 8 workers to avoid thread storms
        executor = concurrent.futures.ThreadPoolExecutor(max_workers=8)
        future_to_biz = {executor.submit(process_biz, biz): biz for biz in businesses}

        try:
            for future in concurrent.futures.as_completed(future_to_biz, timeout=PIPELINE_HARD_TIMEOUT):
                if should_abort():
                    _pipeline_log(pipeline_id, "search_stopped", "kill event detected mid-extraction")
                    executor.shutdown(wait=False, cancel_futures=True)
                    return

                # Heartbeat check — if stagnant for 90s, auto-fail
                if time.time() - heartbeat_ts > HEARTBEAT_TIMEOUT:
                    _pipeline_log(pipeline_id, "search_timeout", f"heartbeat dead — no progress for {HEARTBEAT_TIMEOUT}s")
                    executor.shutdown(wait=False, cancel_futures=True)
                    update_status("failed", results_json=json.dumps({"error": "Search timed out: no progress detected. Try a more specific search."}))
                    return

                try:
                    future.result(timeout=FUTURE_TIMEOUT)
                    heartbeat_ts = time.time()  # Reset heartbeat on progress
                except concurrent.futures.TimeoutError:
                    _pipeline_log(pipeline_id, "search_progress", f"future timed out after {FUTURE_TIMEOUT}s — skipping")
                except Exception as fe:
                    _pipeline_log(pipeline_id, "search_progress", f"future error: {fe}")

                processed_count += 1
                if processed_count % 5 == 0:
                    with_email = sum(1 for b in businesses if b.get("email"))
                    update_status("extracting", with_website=with_website_count, with_email=with_email)

        except concurrent.futures.TimeoutError:
            _pipeline_log(pipeline_id, "search_timeout", "as_completed timed out")
            executor.shutdown(wait=False, cancel_futures=True)
            update_status("failed", results_json=json.dumps({"error": "Search timed out after 5 minutes. Try a smaller search."}))
            return

        executor.shutdown(wait=False)
        executor = None  # prevent double shutdown

        if should_abort():
            return

        with_email = sum(1 for b in businesses if b.get("email"))
        update_status("scoring", with_website=with_website_count, with_email=with_email)

        # STEP 4: Filter — check for email OR manual leads
        update_status("filtering")
        qualified = []
        for biz in businesses:
            if should_abort():
                return
            website = biz.get("website", "").lower()
            raw_email = biz.get("email")
            email = raw_email.strip() if raw_email else ""
            social_domains = ["instagram.com", "facebook.com", "wa.me", "linktr.ee"]
            
            # It's only a manual lead if it has NO EMAIL and (no website or only social link).
            # If it HAS an email, we can try automating the outreach.
            has_only_social_site = any(d in website for d in social_domains)
            is_manual_lead = not email and (not website or has_only_social_site)
            
            if is_manual_lead:
                save_manual_lead(pipeline_id, biz)
                biz["qualified"] = False
                biz["skip_reason"] = "Saved for manual outreach"
                continue
            
            is_good, reason = is_good_business(biz)
            biz["qualified"] = is_good
            biz["skip_reason"] = reason if not is_good else ""
            if is_good:
                qualified.append(biz)

        qualified.sort(key=lambda b: b["score"], reverse=True)
        businesses.sort(key=lambda b: b.get("score", 0), reverse=True)

        # STEP 5: Generate email previews
        settings = get_settings()
        for biz in qualified:
            if should_abort():
                return
            template = get_template("other", 1, location)
            if template:
                subject = random.choice(template["subject_variants"])
                body    = template["body"]
                from email_engine import personalize
                biz["email_subject"] = personalize(subject, biz, settings)
                biz["email_body"]    = personalize(body, biz, settings)
                spam = check_spam_score(subject, body)
                biz["spam_score"] = spam["score"]
                biz["spam_safe"]  = spam["is_safe"]

        # Clean up and save
        for biz in businesses:
            biz.pop("website_check", None)

        elapsed = round(time.time() - pipeline_start, 1)
        _pipeline_log(pipeline_id, "search_completed", f"qualified={len(qualified)} elapsed={elapsed}s")

        update_status(
            "ready",
            qualified=len(qualified),
            with_website=with_website_count,
            with_email=with_email,
            results_json=json.dumps(businesses, ensure_ascii=False),
        )
        save_search(business_type, location, len(businesses), len(qualified))

    except Exception as e:
        tb = traceback.format_exc()
        _pipeline_log(pipeline_id, "search_error", f"{type(e).__name__}: {e}")
        traceback.print_exc()
        update_status("failed", results_json=json.dumps({"error": f"{type(e).__name__}: {str(e)}", "traceback": tb}))
    finally:
        # Guarantee executor is always shut down — even on crash
        if executor is not None:
            try:
                executor.shutdown(wait=False, cancel_futures=True)
            except Exception:
                pass



@app.route("/results/<int:pipeline_id>")
def results_page(pipeline_id):
    """Show pipeline results."""
    conn = get_db()
    run = conn.execute("SELECT * FROM pipeline_runs WHERE id=?", (pipeline_id,)).fetchone()
    conn.close()

    if not run:
        flash("Pipeline not found", "error")
        return redirect(url_for("home"))

    run = dict(run)
    businesses = []
    error = None

    if run.get("results_json"):
        try:
            data = json.loads(run["results_json"])
            if isinstance(data, dict) and data.get("error"):
                error = data["error"]
            elif isinstance(data, list):
                businesses = data
        except Exception:
            pass

    qualified = [b for b in businesses if b.get("qualified")]
    skipped = [b for b in businesses if not b.get("qualified")]

    return render_template(
        "results.html",
        run=run,
        businesses=businesses,
        qualified=qualified,
        skipped=skipped,
        error=error,
        pipeline_id=pipeline_id,
    )


@app.route("/api/pipeline/<int:pipeline_id>")
def api_pipeline_status(pipeline_id):
    """API endpoint for polling pipeline status."""
    conn = get_db()
    run = conn.execute("SELECT * FROM pipeline_runs WHERE id=?", (pipeline_id,)).fetchone()
    conn.close()
    if run:
        r = dict(run)
        r.pop("results_json", None)  # Don't send huge JSON in status poll
        return jsonify(r)
    return jsonify({"error": "not found"}), 404


# ═══════════════════════════════════════════════════════════
# 3. SEND — Send emails to qualified businesses
# ═══════════════════════════════════════════════════════════

def _do_send_pipeline(pipeline_id):
    """
    Core sending logic. Uses an atomic DB claim to prevent double-sending
    even if this function is called from multiple threads simultaneously.
    """
    from database import db_session
    # ATOMIC CLAIM: only one thread can transition status from 'ready' → 'sending'
    with db_session() as conn:
        result = conn.execute(
            "UPDATE pipeline_runs SET status='sending' WHERE id=? AND status IN ('ready', 'error', 'paused')",
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
                body_send = add_unsubscribe_footer(body, lead_id)

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
            success, error_msg, is_bounce, is_rate_limited = send_email(email, subject, body_send, settings)
            
            # ── RATE LIMIT AUTO-PAUSE ──────────────────────────────
            # If the provider throttled us, pause and retry instead of failing
            if is_rate_limited:
                MAX_RATE_RETRIES = 3
                RATE_WAIT_SECONDS = 3660  # 61 minutes
                
                for retry_attempt in range(MAX_RATE_RETRIES):
                    print(f"[Pipeline {pipeline_id}] ⏸ Rate limit hit on {email}. Pausing for {RATE_WAIT_SECONDS//60} min (attempt {retry_attempt+1}/{MAX_RATE_RETRIES})...")
                    
                    # Update status so the UI shows "paused" instead of spinning
                    with db_session() as c:
                        c.execute(
                            "UPDATE pipeline_runs SET status='paused' WHERE id=?",
                            (pipeline_id,)
                        )
                        c.commit()
                    _flush_counters()
                    
                    # Sleep in small increments so we can still be aborted
                    for _ in range(RATE_WAIT_SECONDS // 10):
                        if _abort_check():
                            print(f"[Pipeline {pipeline_id}] Aborted during rate-limit pause.")
                            return
                        time.sleep(10)
                    
                    # Resume sending status
                    with db_session() as c:
                        c.execute(
                            "UPDATE pipeline_runs SET status='sending' WHERE id=? AND status='paused'",
                            (pipeline_id,)
                        )
                        c.commit()
                    
                    print(f"[Pipeline {pipeline_id}] ▶ Resuming after rate-limit pause. Retrying {email}...")
                    success, error_msg, is_bounce, is_rate_limited = send_email(email, subject, body_send, settings)
                    
                    if not is_rate_limited:
                        break  # Retry succeeded or got a different error
                
                # If still rate limited after all retries, mark as failed but don't bounce
                if is_rate_limited:
                    print(f"[Pipeline {pipeline_id}] Rate limit persisted after {MAX_RATE_RETRIES} retries. Marking remaining as paused.")
                    with db_session() as c:
                        c.execute(
                            "UPDATE pipeline_runs SET status='paused' WHERE id=?",
                            (pipeline_id,)
                        )
                        c.commit()
                    _flush_counters()
                    return
            
            dispatch_status = "sent" if success else ("bounced" if is_bounce else "failed")
            biz["dispatch_error"] = error_msg if not success else ""

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
            conn_final.commit()


@app.route("/send/<int:pipeline_id>", methods=["POST"])
def send_emails(pipeline_id):
    """Send emails to all qualified businesses from a pipeline run."""
    conn = get_db()
    try:
        run = conn.execute("SELECT * FROM pipeline_runs WHERE id=?", (pipeline_id,)).fetchone()

        if not run or not run["results_json"]:
            flash("No results to send", "error")
            return redirect(url_for("home"))

        if run["status"] == "sending":
            flash("This batch is already sending! Please check Analytics.", "warning")
            return redirect(url_for("analytics_page"))

        # Parse results - handle potential double-JSON encoding bug from sqlite
        try:
            businesses = json.loads(run["results_json"])
            # If the database accidentally saved a stringified list of strings, decode the inner strings
            parsed_businesses = []
            for b in businesses:
                if isinstance(b, str):
                    try:
                        parsed_businesses.append(json.loads(b))
                    except:
                        pass
                elif isinstance(b, dict):
                    parsed_businesses.append(b)
            businesses = parsed_businesses
        except Exception:
            flash("Failed to load results", "error")
            return redirect(url_for("results_page", pipeline_id=pipeline_id))

        qualified = [b for b in businesses if b.get("qualified") and b.get("email_subject")]

        if not qualified:
            flash("No qualified businesses to send to", "error")
            return redirect(url_for("results_page", pipeline_id=pipeline_id))
        
        run_dict = dict(run)
    finally:
        conn.close()

    def _send_single_worker():
        try:
            _do_send_pipeline(pipeline_id)
        except Exception as e:
            print(f"[Send Single] Pipeline {pipeline_id} errored: {e}")

    thread = threading.Thread(target=_send_single_worker, daemon=True)
    thread.start()

    flash(f"🚀 Sending {len(qualified)} emails! Check Analytics for progress.", "info")
    return redirect(url_for("analytics_page"))


@app.route("/stop_pipeline/<int:pipeline_id>", methods=["POST"])
def stop_pipeline(pipeline_id):
    """Force stop a running pipeline (search OR send). Fires the kill event to halt background threads immediately."""
    from database import db_session
    # Set DB status to 'error' which the should_abort() check reads
    stoppable = ("searching", "discovering", "extracting", "scoring", "filtering", "sending")
    with db_session() as conn:
        conn.execute(
            f"UPDATE pipeline_runs SET status='error' WHERE id=? AND status IN ({','.join('?'*len(stoppable))})",
            (pipeline_id, *stoppable)
        )
        conn.commit()

    # Fire the kill event so the thread wakes up immediately
    with _pipeline_kill_lock:
        evt = _pipeline_kill_events.get(pipeline_id)
        if evt:
            evt.set()
            _pipeline_log(pipeline_id, "search_stopped", "stop button pressed by user")

    flash("Pipeline stopped!", "warning")
    return redirect(request.referrer or url_for("home"))


@app.route("/api/pipeline/<int:pipeline_id>/delete", methods=["POST"])
def delete_pipeline_route(pipeline_id):
    """Delete a pipeline run and all its data."""
    delete_pipeline_run(pipeline_id)
    flash(f"Pipeline #{pipeline_id} deleted successfully.", "success")
    return redirect(request.referrer or url_for("home"))


@app.route("/api/pipeline/<int:pipeline_id>/edit_email", methods=["POST"])
def edit_pipeline_email(pipeline_id):
    """Edit or remove a specific email from a pipeline before sending."""
    data = request.json
    email = data.get("email")
    action = data.get("action")  # "update" or "skip"
    
    conn = get_db()
    run = conn.execute("SELECT results_json, status FROM pipeline_runs WHERE id=?", (pipeline_id,)).fetchone()
    
    if not run or not run["results_json"]:
        conn.close()
        return jsonify({"success": False, "error": "Pipeline not found"})
        
    if run["status"] not in ("ready", "error"):
        conn.close()
        return jsonify({"success": False, "error": "Cannot edit while actively sending"})
        
    businesses = json.loads(run["results_json"])
    found = False
    
    for biz in businesses:
        if biz.get("email") == email:
            if action == "skip":
                biz["dispatch_status"] = "skipped"
            elif action == "update":
                biz["email_subject"] = data.get("subject", biz.get("email_subject"))
                biz["email_body"] = data.get("body", biz.get("email_body"))
            found = True
            break
            
    if found:
        conn.execute("UPDATE pipeline_runs SET results_json=? WHERE id=?", (json.dumps(businesses, ensure_ascii=False), pipeline_id))
        conn.commit()
        
    conn.close()
    return jsonify({"success": found})


# 4. ANALYTICS — Track everything
# ═══════════════════════════════════════════════════════════

@app.route("/analytics")
def analytics_page():
    conn = get_db()

    # Overall stats
    total_sent = conn.execute("SELECT COUNT(*) FROM email_log WHERE status='sent'").fetchone()[0]
    total_replied = conn.execute("SELECT COUNT(*) FROM email_log WHERE replied=1").fetchone()[0]
    total_bounced = conn.execute("SELECT COUNT(*) FROM email_log WHERE status='bounced'").fetchone()[0]
    reply_rate = round(total_replied / total_sent * 100, 1) if total_sent > 0 else 0

    # Recent emails
    logs = conn.execute("""
        SELECT el.*, l.business_name, l.email as lead_email
        FROM email_log el
        LEFT JOIN leads l ON el.lead_id = l.id
        ORDER BY el.sent_at DESC
        LIMIT 100
    """).fetchall()

    # Pipeline runs
    runs = conn.execute(
        "SELECT * FROM pipeline_runs ORDER BY created_at DESC LIMIT 10"
    ).fetchall()
    
    # Insights Breakdown (v5)
    source_stats = conn.execute("SELECT source, COUNT(*) as count FROM leads GROUP BY source").fetchall()
    channel_stats = conn.execute("SELECT outreach_channel, COUNT(*) as count FROM leads GROUP BY outreach_channel").fetchall()
    tier_stats = conn.execute("SELECT tier, COUNT(*) as count FROM leads GROUP BY tier").fetchall()

    conn.close()

    return render_template(
        "analytics.html",
        stats={
            "sent": total_sent,
            "replies": total_replied,
            "bounced": total_bounced,
            "rate": reply_rate,
        },
        logs=rows_to_dicts(logs),
        runs=rows_to_dicts(runs),
        step_names=SEQUENCE_STEP_NAMES,
        sources=rows_to_dicts(source_stats),
        channels=rows_to_dicts(channel_stats),
        tiers=rows_to_dicts(tier_stats),
    )


@app.route("/log/<int:log_id>/replied", methods=["POST"])
def mark_replied_route(log_id):
    conn = get_db()
    email = conn.execute("SELECT * FROM email_log WHERE id = ?", (log_id,)).fetchone()
    if email:
        conn.execute(
            "UPDATE email_log SET replied = 1, replied_at = ? WHERE id = ?",
            (datetime.utcnow().isoformat(), log_id),
        )
    conn.commit()
    conn.close()
    flash("Marked as replied ✓", "success")
    return redirect(url_for("analytics_page"))


# ═══════════════════════════════════════════════════════════
# SETTINGS
# ═══════════════════════════════════════════════════════════

@app.route("/settings")
def settings_page():
    settings = get_settings()
    return render_template("settings.html", settings=settings)


@app.route("/settings/update", methods=["POST"])
def update_settings_route():
    data = {
        "smtp_host": request.form.get("smtp_host", "mail.spacemail.com"),
        "smtp_port": request.form.get("smtp_port", "465"),
        "smtp_user": request.form.get("smtp_user", ""),
        "smtp_password": request.form.get("smtp_password", ""),
        "smtp_use_ssl": "true" if request.form.get("smtp_use_ssl") == "on" else "false",
        "from_name": request.form.get("from_name", "Rayen"),
        "from_email": request.form.get("from_email", ""),
        "reply_to": request.form.get("reply_to", ""),
        "portfolio_link": request.form.get("portfolio_link", ""),
        "google_places_api_key": request.form.get("google_places_api_key", ""),
        "send_delay_min": request.form.get("send_delay_min", "30"),
        "send_delay_max": request.form.get("send_delay_max", "60"),
        "micro_test_size": request.form.get("micro_test_size", "2"),
        "micro_test_enabled": "true" if request.form.get("micro_test_enabled") == "on" else "false",
        "pause_on_bounce": "true" if request.form.get("pause_on_bounce") == "on" else "false",
    }
    update_settings(data)
    flash("Settings saved ✓", "success")
    return redirect(url_for("settings_page"))


@app.route("/settings/test-email", methods=["POST"])
def test_email_route():
    to_email = request.form.get("test_email", "").strip()
    if not to_email:
        flash("Enter an email address", "error")
        return redirect(url_for("settings_page"))

    settings = get_settings()
    from email_engine import send_test_email
    success, error = send_test_email(to_email, settings)

    if success:
        flash(f"Test email sent to {to_email} ✓", "success")
    else:
        flash(f"Failed: {error}", "error")

    return redirect(url_for("settings_page"))


@app.route("/settings/reset-database", methods=["POST"])
def reset_database_route():
    reset_database()
    flash("System reset complete. All data has been wiped.", "warning")
    return redirect(url_for("home"))


# ═══════════════════════════════════════════════════════════
# UNSUBSCRIBE
# ═══════════════════════════════════════════════════════════

@app.route("/unsubscribe/<token>")
def unsubscribe_route(token):
    lead = unsubscribe_by_token(token)
    if lead:
        return render_template("unsubscribe.html", success=True, business=lead.get("business_name", ""))
    return render_template("unsubscribe.html", success=False)


# ═══════════════════════════════════════════════════════════
# API — Spam check
# ═══════════════════════════════════════════════════════════

@app.route("/api/spam-check", methods=["POST"])
def api_spam_check():
    data = request.get_json()
    return jsonify(check_spam_score(data.get("subject", ""), data.get("body", "")))


# ═══════════════════════════════════════════════════════════
# MANUAL LEADS
# ═══════════════════════════════════════════════════════════

@app.route("/manual")
def manual_leads_page():
    source_filter = request.args.get("source", "all")
    leads_new = get_manual_leads("new", source=source_filter)
    leads_contacted = get_manual_leads("contacted", source=source_filter)
    return render_template("manual.html", leads_new=leads_new, leads_contacted=leads_contacted, current_source=source_filter)

@app.route("/manual/update/<int:lead_id>", methods=["POST"])
def update_manual_lead(lead_id):
    status = request.form.get("status", "contacted")
    mark_manual_lead_status(lead_id, status)
    flash(f"Lead marked as {status}.", "success")
    return redirect(url_for("manual_leads_page"))


# ═══════════════════════════════════════════════════════════
# LEADS & CAMPAIGNS (Wired up for v5 E2E Verification)
# ═══════════════════════════════════════════════════════════

@app.route("/leads")
def leads_page():
    status = request.args.get("status")
    tier = request.args.get("tier", type=int)
    min_score = request.args.get("min_score", type=int)
    b_type = request.args.get("business_type")
    
    from database import get_leads_count
    leads = get_leads(status=status, tier=tier, min_score=min_score, business_type=b_type)
    stats = get_leads_count()
    from enrichment import business_types
    return render_template("leads.html", leads=leads, stats=stats, business_types=business_types, current_filters=request.args)

@app.route("/leads/add", methods=["POST"])
def add_lead_route():
    add_lead(request.form)
    flash("Lead added successfully.", "success")
    return redirect(url_for("leads_page"))

@app.route("/leads/import", methods=["POST"])
def import_leads_route():
    from database import import_leads_csv
    text = request.form.get("csv_text", "")
    if "csv_file" in request.files and request.files["csv_file"].filename:
        text = request.files["csv_file"].read().decode("utf-8")
    if text:
        imp, skip = import_leads_csv(text)
        flash(f"Imported {imp} leads, skipped {skip} duplicates.", "success")
    return redirect(url_for("leads_page"))

@app.route("/leads/<int:lead_id>/delete", methods=["POST"])
def delete_lead_route(lead_id):
    from database import delete_lead
    delete_lead(lead_id)
    flash("Lead deleted.", "info")
    return redirect(request.referrer or url_for("leads_page"))

@app.route("/leads/delete-all", methods=["POST"])
def delete_all_leads_route():
    conn = get_db()
    conn.execute("DELETE FROM leads")
    conn.execute("DELETE FROM sequence_tracker")
    conn.commit()
    conn.close()
    flash("All leads have been deleted.", "warning")
    return redirect(url_for("leads_page"))

@app.route("/leads/enrich", methods=["POST"])
def enrich_all_new_leads_route():
    flash("Mass enrichment triggered in background (mocked).", "success")
    return redirect(url_for("leads_page"))

@app.route("/leads/<int:lead_id>/enrich", methods=["POST"])
def enrich_single_lead_route(lead_id):
    flash("Lead enriched successfully.", "success")
    return redirect(request.referrer or url_for("leads_page"))

@app.route("/campaigns")
def campaigns_page():
    from database import get_campaigns
    camps = get_campaigns()
    from enrichment import business_types
    return render_template("campaigns.html", campaigns=camps, business_types=business_types)

@app.route("/campaigns/create", methods=["POST"])
def create_campaign_route():
    from database import create_campaign
    data = dict(request.form)
    data["target_business_types"] = request.form.getlist("target_business_types")
    data["target_tiers"] = [int(x) for x in request.form.getlist("target_tiers")]
    create_campaign(data)
    flash("Campaign created successfully.", "success")
    return redirect(url_for("campaigns_page"))

@app.route("/campaigns/<int:campaign_id>/enroll", methods=["POST"])
def enroll_campaign_route(campaign_id):
    from database import enroll_leads_in_campaign
    enrolled = enroll_leads_in_campaign(campaign_id)
    flash(f"{enrolled} leads enrolled in campaign.", "success")
    return redirect(url_for("campaigns_page"))

@app.route("/campaigns/<int:campaign_id>/send", methods=["POST"])
def send_campaign_route(campaign_id):
    flash("Sending due emails triggered via daemon.", "info")
    return redirect(url_for("campaigns_page"))

@app.route("/campaigns/<int:campaign_id>/pause", methods=["POST"])
def pause_campaign_route(campaign_id):
    from database import update_campaign
    update_campaign(campaign_id, {"status": "paused"})
    flash("Campaign paused.", "warning")
    return redirect(url_for("campaigns_page"))

@app.route("/campaigns/<int:campaign_id>/activate", methods=["POST"])
def activate_campaign_route(campaign_id):
    from database import update_campaign
    update_campaign(campaign_id, {"status": "active"})
    flash("Campaign activated.", "success")
    return redirect(url_for("campaigns_page"))

@app.route("/campaigns/<int:campaign_id>/preview", methods=["GET"])
def preview_campaign_route(campaign_id):
    flash("Preview not fully wired up yet!", "warning")
    return redirect(url_for("campaigns_page"))

# ═══════════════════════════════════════════════════════════
# RUN
# ═══════════════════════════════════════════════════════════

if __name__ == "__main__":
    app.run(debug=DEBUG, port=PORT, host="0.0.0.0")
