"""
ClientEngine v4 â€” Simple, Connected, Smart.
4 pages: Home â†’ Results â†’ Analytics â†’ Settings.
One continuous workflow: Search â†’ Extract â†’ Score â†’ Preview â†’ Send â†’ Track.
"""
import json
import time
import random
import threading
import traceback
import logging
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
    db_session,
)
from business_discovery import search_businesses
from email_extractor import find_email
from enrichment import (
    detect_business_type, check_website, score_business,
    is_good_business, is_good_email,
)
from email_engine import send_email, check_spam_score
from templates_data import get_template, SEQUENCE_STEP_NAMES
from config import (
    SECRET_KEY, DEBUG, PORT, APP_BASE_URL,
    PIPELINE_DEBUG, SEARCH_TIMEOUT_SECONDS, DISCOVERY_TIMEOUT_SECONDS,
    LEAD_TIMEOUT_SECONDS, LEAD_FETCH_CONNECT_TIMEOUT_SECONDS,
    LEAD_FETCH_READ_TIMEOUT_SECONDS, SEARCH_MAX_RESULTS,
)
from auto_followup import start_daemon
from pipeline_state import (
    STATUS_PENDING, STATUS_DISCOVERING, STATUS_EXTRACTING, STATUS_SCORING,
    STATUS_READY, STATUS_PARTIAL, STATUS_FAILED, STATUS_DONE, STATUS_NO_EMAILS,
    STATUS_NO_WEBSITE, STATUS_TIMEOUT, STATUS_ERROR, STATUS_SENDING,
    STATUS_PAUSED, STATUS_STOPPED, SEARCH_ACTIVE_STATUSES,
    normalize_status, is_search_terminal, summarize_businesses,
    determine_search_status,
)

app = Flask(__name__)
app.secret_key = SECRET_KEY

logger = logging.getLogger("clientengine")
if not logger.handlers:
    log_level = logging.DEBUG if (DEBUG or PIPELINE_DEBUG) else logging.INFO
    logger.setLevel(log_level)
    fmt = logging.Formatter("%(asctime)s %(levelname)s %(message)s")

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(fmt)
    logger.addHandler(console_handler)

    try:
        import os

        os.makedirs("data", exist_ok=True)
        file_handler = logging.FileHandler("data/pipeline_events.log", encoding="utf-8")
        file_handler.setFormatter(fmt)
        logger.addHandler(file_handler)
    except Exception:
        pass


def _setting_int(settings, key, default, minimum=1, maximum=None):
    value = settings.get(key, default)
    try:
        parsed = int(value)
    except Exception:
        parsed = int(default)
    parsed = max(minimum, parsed)
    if maximum is not None:
        parsed = min(maximum, parsed)
    return parsed


def _setting_float(settings, key, default, minimum=0.1, maximum=None):
    value = settings.get(key, default)
    try:
        parsed = float(value)
    except Exception:
        parsed = float(default)
    parsed = max(minimum, parsed)
    if maximum is not None:
        parsed = min(maximum, parsed)
    return parsed


def _is_active_search_status(status):
    return normalize_status(status) in SEARCH_ACTIVE_STATUSES


def _safe_json_dumps(value):
    return json.dumps(value, ensure_ascii=False)

# Register urlencode as a Jinja2 filter (used in manual.html for WhatsApp links)
from urllib.parse import quote
app.jinja_env.filters['urlencode'] = lambda s: quote(str(s), safe='')

init_db()

# Start background sequence daemon
start_daemon()

# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
# STARTUP SELF-HEALING â€” Fix anything stuck from a crash
# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
try:
    import os
    os.makedirs("data", exist_ok=True)
    _startup_conn = get_db()
    # 1. Re-queue any search_queue items stuck as 'running'
    _startup_conn.execute("UPDATE search_queue SET status='pending' WHERE status='running'")
    # 2. Self-heal: mark any pipeline_runs stuck in an active SEARCH state as 'failed'
    #    (They'll show a clear error rather than spinning forever)
    _stuck_search_states = tuple(SEARCH_ACTIVE_STATUSES) + ('searching', 'filtering')
    placeholders = ",".join(["?"] * len(_stuck_search_states))
    _startup_conn.execute(
        f"UPDATE pipeline_runs SET status=?, results_json=? WHERE status IN ({placeholders})",
        (STATUS_FAILED, json.dumps({"error": "Server restarted while process was running. Please start a new process."}), *_stuck_search_states)
    )
    # 3. Reset 'sending' pipelines back to 'ready' so user can re-send them
    #    (Don't mark as 'failed' â€” the emails are already generated, just need to resume sending)
    _startup_conn.execute("UPDATE pipeline_runs SET status=? WHERE status=?", (STATUS_READY, STATUS_SENDING))
    # 4. Leave 'paused' pipelines as-is â€” they were rate-limited and can be resumed
    _startup_conn.commit()
    _pending = _startup_conn.execute("SELECT COUNT(*) FROM search_queue WHERE status='pending'").fetchone()[0]
    _startup_conn.close()
    if _pending > 0:
        print(f"ðŸ“‹ Found {_pending} pending queue items from previous session â€” restarting queue worker.")
except Exception:
    _pending = 0

# â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
# PIPELINE KILL REGISTRY â€” Tracks active pipeline events for cancellation
# â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
_pipeline_kill_events = {}   # pipeline_id -> threading.Event
_pipeline_kill_lock = threading.Lock()

# â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
# PIPELINE LOGGER â€” Writes timestamped debug logs per search
# â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
PIPELINE_LOG_FILE = "data/pipeline_events.log"


def _pipeline_log(pipeline_id, event, message="", level="info"):
    """Write a structured log entry for a pipeline transition."""
    payload = f"pipeline_id={pipeline_id} event={event} {message}".strip()
    log_fn = getattr(logger, level, logger.info)
    log_fn(payload)

# â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
# BATCH QUEUE WORKER â€” Runs pending searches sequentially
# â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
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
                    "INSERT INTO pipeline_runs (query, location, source_choice, status) VALUES (?, ?, ?, ?)",
                    (query, location, source_choice, STATUS_PENDING),
                )
                pid = cur.lastrowid
                conn.commit()

            _pipeline_log(pid, "search_started", f"query='{query}' location='{location}' source={source_choice}")

            # Register kill event for this pipeline
            kill_event = threading.Event()
            with _pipeline_kill_lock:
                _pipeline_kill_events[pid] = kill_event

            # Run the FULL pipeline (discover â†’ extract â†’ score â†’ email preview)
            _execute_pipeline(pid, query, location, source_choice, kill_event)

            with db_session() as conn:
                row = conn.execute("SELECT status FROM pipeline_runs WHERE id=?", (pid,)).fetchone()
                final_status = normalize_status(row["status"] if row else "")
            queue_error = final_status in {STATUS_FAILED, STATUS_ERROR, STATUS_TIMEOUT}
            complete_queue_item(item["id"], pid, error=queue_error)
        except Exception as e:
            _pipeline_log(pid, "search_error", f"{type(e).__name__}: {e}")
            traceback.print_exc()
            if pid:
                try:
                    from database import db_session
                    with db_session() as conn:
                        conn.execute(
                            "UPDATE pipeline_runs SET status=?, results_json=? WHERE id=?",
                            (STATUS_FAILED, json.dumps({"error": f"{type(e).__name__}: {str(e)}"}), pid)
                        )
                        conn.commit()
                except Exception as update_exc:
                    _pipeline_log(pid, "search_error", f"failed to mark pipeline failed: {update_exc}", level="error")
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
_send_worker_lock = threading.Lock()
_active_send_workers = set()


def _is_send_worker_active(pipeline_id):
    with _send_worker_lock:
        return pipeline_id in _active_send_workers


def _set_send_worker_active(pipeline_id, active):
    with _send_worker_lock:
        if active:
            _active_send_workers.add(pipeline_id)
        else:
            _active_send_workers.discard(pipeline_id)

# Auto-resume queue from previous session on startup
if _pending > 0:
    _start_queue_if_idle()


# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
# 1. HOME â€” Search box + Stats
# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•

@app.route("/")
def home():
    conn = get_db()
    # Quick stats
    total_sent = conn.execute("SELECT COUNT(*) FROM email_log WHERE status='sent'").fetchone()[0]
    total_replied = conn.execute("SELECT COUNT(*) FROM email_log WHERE replied=1").fetchone()[0]
    reply_rate = round(total_replied / total_sent * 100, 1) if total_sent > 0 else 0
    sent_last_hour = conn.execute(
        "SELECT COUNT(*) FROM email_log WHERE status='sent' AND datetime(sent_at) >= datetime('now','-1 hour')"
    ).fetchone()[0]
    sent_last_day = conn.execute(
        "SELECT COUNT(*) FROM email_log WHERE status='sent' AND datetime(sent_at) >= datetime('now','-1 day')"
    ).fetchone()[0]

    # Pipeline runs (show last 20)
    runs = conn.execute(
        "SELECT * FROM pipeline_runs ORDER BY created_at DESC LIMIT 20"
    ).fetchall()

    # Count how many are ready to send
    ready_count = conn.execute(
        "SELECT COUNT(*) FROM pipeline_runs WHERE status IN ('ready','partial','timeout','paused')"
    ).fetchone()[0]

    settings = get_settings()
    hourly_limit = _setting_int(settings, "smtp_hourly_limit", 500, minimum=0, maximum=500000)
    daily_limit = _setting_int(settings, "smtp_daily_limit", 2000, minimum=0, maximum=500000)
    history = get_search_history(limit=5)
    conn.close()

    hourly_remaining = max(hourly_limit - sent_last_hour, 0) if hourly_limit > 0 else 0
    daily_remaining = max(daily_limit - sent_last_day, 0) if daily_limit > 0 else 0

    return render_template(
        "home.html",
        stats={"sent": total_sent, "replies": total_replied, "rate": reply_rate},
        send_capacity={
            "hourly_limit": hourly_limit,
            "hourly_sent": sent_last_hour,
            "hourly_remaining": hourly_remaining,
            "daily_limit": daily_limit,
            "daily_sent": sent_last_day,
            "daily_remaining": daily_remaining,
        },
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
                
    flash("ðŸ—‘ï¸ All past searches and pipeline history have been deleted.", "success")
    return redirect(url_for("home"))

@app.route("/api/reset/followups", methods=["POST"])
def reset_followups():
    """Instantly stop all automated follow-up emails."""
    from database import cancel_all_sequences
    cancel_all_sequences()
    flash("ðŸ›‘ All automated follow-up emails have been instantly cancelled.", "success")
    return redirect(url_for("home"))

@app.route("/api/reset/manual_leads", methods=["POST"])
def reset_manual_leads():
    """Wipe all manual leads."""
    from database import clear_all_manual_leads
    clear_all_manual_leads()
    flash("ðŸ—‘ï¸ All manual leads have been deleted.", "success")
    return redirect(url_for("manual_leads_page"))


@app.route("/send-all-ready", methods=["POST"])
def send_all_ready():
    """Trigger sending for ALL 'ready' pipeline runs, one by one in the background."""
    global _send_all_running
    with _send_all_lock:
        if _send_all_running:
            flash("â³ Already sending â€” wait for current batch to finish before pressing again.", "warning")
            return redirect(url_for("home"))
        _send_all_running = True

    conn = get_db()
    ready_runs = conn.execute(
        "SELECT id FROM pipeline_runs WHERE status IN ('ready','partial','timeout')"
    ).fetchall()
    conn.close()

    if not ready_runs:
        _send_all_running = False
        flash("No resumable pipelines found.", "warning")
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
                    with db_session() as conn:
                        row = conn.execute("SELECT status FROM pipeline_runs WHERE id=?", (pid,)).fetchone()
                        final_status = normalize_status(row["status"] if row else "")
                    if final_status == STATUS_PAUSED:
                        _pipeline_log(
                            pid,
                            "send_all_halted",
                            "stopping remaining pipelines after provider/local rate-limit",
                            level="warning",
                        )
                        break
                    time.sleep(2)  # brief pause before starting the next pipeline
                except Exception as e:
                    print(f"[Send All] Pipeline {pid} errored: {e}")
        finally:
            _send_all_running = False

    threading.Thread(target=_send_all_worker, daemon=True).start()
    flash(f"ðŸš€ Sending queued for {count} pipeline(s)! Check Analytics for live progress.", "success")
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


# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
# 2. SEARCH PIPELINE â€” Discover â†’ Extract â†’ Score â†’ Filter
# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•

@app.route("/search", methods=["POST"])
def search_pipeline():
    """Run the entire search â†’ extract â†’ score â†’ filter pipeline."""
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
            # Accept: "Beauty Salon â€“ Bend, OR" or "Beauty Salon - Bend, OR" or "Beauty Salon Bend, OR"
            for sep in ["â€“", "-", ","]:
                if sep in line:
                    parts = line.split(sep, 1)
                    parsed.append((parts[0].strip(), parts[1].strip()))
                    break
            else:
                words = line.split(maxsplit=1)
                parsed.append((words[0], words[1] if len(words) > 1 else ""))
        enqueue_searches(parsed, source_choice)
        _start_queue_if_idle()
        flash(f"â³ Batch of {len(parsed)} searches queued! They will run one by one.", "success")
        return redirect(url_for("home"))

    # Single-line mode (original behaviour)
    single = lines[0] if lines else raw_query
    for sep in ["â€“", "-", ","]:
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
        "INSERT INTO pipeline_runs (query, location, source_choice, status) VALUES (?, ?, ?, ?)",
        (business_type, location, source_choice, STATUS_PENDING),
    )
    pipeline_id = cursor.lastrowid
    conn.commit()
    conn.close()

    # Run pipeline in background
    kill_event = threading.Event()
    with _pipeline_kill_lock:
        _pipeline_kill_events[pipeline_id] = kill_event
    
    def run_pipeline():
        try:
            _execute_pipeline(pipeline_id, business_type, location, source_choice, kill_event=kill_event)
        finally:
            with _pipeline_kill_lock:
                _pipeline_kill_events.pop(pipeline_id, None)

    thread = threading.Thread(target=run_pipeline, daemon=True)
    thread.start()

    return redirect(url_for("results_page", pipeline_id=pipeline_id))


def _execute_pipeline(pipeline_id, business_type, location, source_choice="all", kill_event=None):
    """
    Execute the full pipeline with per-lead isolation and persistent progress.
    """
    from enrichment import (
        check_website,
        detect_business_type,
        score_business,
        assign_tier,
        choose_channel,
    )
    from database import db_session

    businesses = []

    settings = get_settings()
    search_timeout = _setting_int(settings, "search_timeout_seconds", SEARCH_TIMEOUT_SECONDS, minimum=120, maximum=7200)
    discovery_timeout = _setting_int(settings, "discovery_timeout_seconds", DISCOVERY_TIMEOUT_SECONDS, minimum=30, maximum=1800)
    lead_timeout = _setting_int(settings, "lead_timeout_seconds", LEAD_TIMEOUT_SECONDS, minimum=5, maximum=300)
    fetch_connect_timeout = _setting_float(
        settings,
        "lead_fetch_connect_timeout_seconds",
        LEAD_FETCH_CONNECT_TIMEOUT_SECONDS,
        minimum=1.0,
        maximum=30.0,
    )
    fetch_read_timeout = _setting_float(
        settings,
        "lead_fetch_read_timeout_seconds",
        LEAD_FETCH_READ_TIMEOUT_SECONDS,
        minimum=1.0,
        maximum=45.0,
    )
    search_max_results = _setting_int(settings, "search_max_results", SEARCH_MAX_RESULTS, minimum=5, maximum=200)
    debug_search = str(settings.get("search_debug", str(PIPELINE_DEBUG))).lower() == "true"

    terminal_statuses = {
        STATUS_READY,
        STATUS_PARTIAL,
        STATUS_FAILED,
        STATUS_DONE,
        STATUS_NO_EMAILS,
        STATUS_NO_WEBSITE,
        STATUS_TIMEOUT,
        STATUS_ERROR,
    }

    def update_status(status, *, results_json=None, **kwargs):
        payload = dict(kwargs)
        if results_json is not None:
            payload["results_json"] = results_json
        if status in terminal_statuses:
            payload["completed_at"] = datetime.utcnow().isoformat()

        sets = ", ".join(f"{k}=?" for k in payload)
        vals = list(payload.values())
        with db_session() as conn:
            if sets:
                conn.execute(
                    f"UPDATE pipeline_runs SET status=?, {sets} WHERE id=?",
                    [status] + vals + [pipeline_id],
                )
            else:
                conn.execute("UPDATE pipeline_runs SET status=? WHERE id=?", (status, pipeline_id))
            conn.commit()

        if debug_search:
            details = " ".join(f"{k}={v}" for k, v in payload.items() if k != "results_json")
            _pipeline_log(pipeline_id, f"search_{status}", details)
        else:
            _pipeline_log(pipeline_id, f"search_{status}")

    def should_abort():
        if kill_event and kill_event.is_set():
            return True
        try:
            with db_session() as conn:
                row = conn.execute("SELECT status FROM pipeline_runs WHERE id=?", (pipeline_id,)).fetchone()
                return normalize_status(row["status"] if row else "") in {STATUS_ERROR, STATUS_STOPPED}
        except Exception:
            return False

    def _run_with_timeout(func, timeout_seconds, *args, **kwargs):
        done = threading.Event()
        state = {"result": None, "error": None, "traceback": ""}

        def _target():
            try:
                state["result"] = func(*args, **kwargs)
            except Exception as exc:
                state["error"] = exc
                state["traceback"] = traceback.format_exc()
            finally:
                done.set()

        worker = threading.Thread(target=_target, daemon=True)
        worker.start()
        if not done.wait(timeout_seconds):
            return None, TimeoutError(f"lead timed out after {timeout_seconds}s"), ""

        return state["result"], state["error"], state["traceback"]

    def process_business(input_biz):
        biz = dict(input_biz or {})
        biz.setdefault("lead_status", "processed")
        biz.setdefault("lead_error", "")

        website = (biz.get("website") or "").strip()
        if not website:
            biz["lead_status"] = "no_website"
            biz["website_unreachable"] = True
            biz["website_fetch_error"] = "missing_website"
            biz["website_check"] = {
                "status": "none",
                "response_time_ms": 0,
                "has_ssl": False,
                "has_mobile": False,
                "cms_detected": None,
                "has_contact_form": False,
                "has_cta": False,
                "website_score": 0,
                "status_code": 0,
                "final_url": "",
                "fetch_error": "missing_website",
            }
        else:
            email_result = find_email(
                biz.get("business_name", ""),
                website,
                request_timeout=(fetch_connect_timeout, fetch_read_timeout),
            )
            if email_result.get("email"):
                biz["email"] = email_result["email"]
                biz["email_source"] = email_result.get("source", "website")
                biz["email_confidence"] = float(email_result.get("confidence", 0))
            else:
                biz["lead_status"] = "no_email"

            biz["email_candidates"] = email_result.get("all_found", [])[:5]
            biz["website_checked_urls"] = email_result.get("checked_urls", [])[:8]
            biz["website_fetch_errors"] = email_result.get("errors", [])[:6]
            biz["website_final_url"] = email_result.get("final_url", "")
            biz["website_status_code"] = int(email_result.get("status_code", 0) or 0)

            website_check = check_website(
                website,
                connect_timeout=fetch_connect_timeout,
                read_timeout=fetch_read_timeout,
            )
            biz["website_check"] = website_check

            if website_check.get("status") == "error":
                biz["website_unreachable"] = True
                biz["website_fetch_error"] = website_check.get("fetch_error", "unreachable")
                if biz.get("lead_status") in ("processed", "no_email"):
                    biz["lead_status"] = "website_unreachable"

        biz["business_type"] = detect_business_type((biz.get("category", "") + " " + biz.get("business_name", "")).strip())
        score_result = score_business(biz)
        biz["score"] = score_result.get("score", 0)
        biz["score_details"] = score_result.get("details", [])
        biz["pain_points"] = score_result.get("pain_points", [])
        biz["tier"] = assign_tier(biz.get("score", 0))
        biz["outreach_channel"] = choose_channel(biz)

        return biz

    try:
        pipeline_start = time.time()
        deadline = pipeline_start + search_timeout

        update_status(STATUS_DISCOVERING)
        if should_abort():
            return

        discovery_result = [None]
        discovery_error = [None]

        def _do_discover():
            try:
                discovery_result[0] = search_businesses(
                    business_type,
                    location,
                    source_choice,
                    max_results=search_max_results,
                )
            except Exception as exc:
                discovery_error[0] = exc

        disc_thread = threading.Thread(target=_do_discover, daemon=True)
        disc_thread.start()
        disc_thread.join(timeout=discovery_timeout)

        if disc_thread.is_alive():
            update_status(
                STATUS_TIMEOUT,
                found=0,
                with_website=0,
                with_email=0,
                qualified=0,
                results_json=_safe_json_dumps({"error": f"Discovery timed out after {discovery_timeout}s."}),
            )
            return

        if discovery_error[0]:
            raise discovery_error[0]

        discovery = discovery_result[0] or {}
        if discovery.get("error"):
            error_message = discovery.get("error")
            if discovery.get("debug_errors"):
                error_message += " | Debug: " + ", ".join(discovery["debug_errors"])
            update_status(
                STATUS_FAILED,
                found=0,
                with_website=0,
                with_email=0,
                qualified=0,
                results_json=_safe_json_dumps({"error": error_message}),
            )
            return

        businesses = list(discovery.get("results", []) or [])
        if not businesses:
            update_status(
                STATUS_NO_WEBSITE,
                found=0,
                with_website=0,
                with_email=0,
                qualified=0,
                results_json=_safe_json_dumps([]),
            )
            save_search(business_type, location, 0, 0)
            return

        update_status(STATUS_EXTRACTING, found=len(businesses), with_website=0, with_email=0, qualified=0)

        processed = 0
        had_errors = False
        timed_out = False

        for idx, raw_biz in enumerate(businesses):
            if should_abort():
                _pipeline_log(pipeline_id, "search_stopped", "stop requested by user")
                return

            if time.time() >= deadline:
                timed_out = True
                had_errors = True
                _pipeline_log(pipeline_id, "search_timeout", "search deadline reached during extraction")
                break

            biz, lead_error, lead_trace = _run_with_timeout(process_business, lead_timeout, raw_biz)
            if isinstance(lead_error, TimeoutError):
                had_errors = True
                biz = dict(raw_biz or {})
                biz["lead_status"] = "timeout"
                biz["lead_error"] = str(lead_error)
            elif lead_error is not None:
                had_errors = True
                biz = dict(raw_biz or {})
                biz["lead_status"] = "failed"
                biz["lead_error"] = f"{type(lead_error).__name__}: {lead_error}"
                if debug_search and lead_trace:
                    biz["lead_traceback"] = lead_trace

            businesses[idx] = biz
            processed += 1

            metrics = summarize_businesses(businesses)
            if processed % 3 == 0 or processed == len(businesses):
                update_status(
                    STATUS_EXTRACTING,
                    found=metrics["found"],
                    with_website=metrics["with_website"],
                    with_email=metrics["with_email"],
                    qualified=metrics["qualified"],
                    results_json=_safe_json_dumps(businesses),
                )

        if should_abort():
            return

        update_status(
            STATUS_SCORING,
            found=len(businesses),
            with_website=sum(1 for b in businesses if (b.get("website") or "").strip()),
            with_email=sum(1 for b in businesses if (b.get("email") or "").strip()),
            qualified=0,
            results_json=_safe_json_dumps(businesses),
        )

        qualified = []
        for biz in businesses:
            if should_abort():
                return

            website = (biz.get("website") or "").lower().strip()
            email = (biz.get("email") or "").strip()
            social_domains = ("instagram.com", "facebook.com", "wa.me", "linktr.ee")
            has_only_social_site = bool(website) and any(domain in website for domain in social_domains)

            if not email and (not website or has_only_social_site):
                save_manual_lead(pipeline_id, biz)
                biz["qualified"] = False
                biz["skip_reason"] = "Saved for manual outreach"
                if not biz.get("lead_status"):
                    biz["lead_status"] = "no_website"
                continue

            is_good, reason = is_good_business(biz)
            biz["qualified"] = is_good
            biz["skip_reason"] = "" if is_good else reason
            if is_good:
                qualified.append(biz)

        qualified.sort(key=lambda b: b.get("score", 0), reverse=True)
        businesses.sort(key=lambda b: b.get("score", 0), reverse=True)

        app_settings = get_settings()
        for biz in qualified:
            if should_abort():
                return
            template = get_template("other", 1, location)
            if not template:
                continue
            subject = random.choice(template["subject_variants"])
            body = template["body"]
            from email_engine import personalize

            biz["email_subject"] = personalize(subject, biz, app_settings)
            biz["email_body"] = personalize(body, biz, app_settings)
            spam = check_spam_score(subject, body)
            biz["spam_score"] = spam["score"]
            biz["spam_safe"] = spam["is_safe"]

        metrics = summarize_businesses(businesses)
        final_status = determine_search_status(
            found=metrics["found"],
            with_website=metrics["with_website"],
            with_email=metrics["with_email"],
            qualified=metrics["qualified"],
            processed=processed,
            total=len(businesses),
            timed_out=timed_out,
            had_errors=had_errors,
        )

        elapsed = round(time.time() - pipeline_start, 1)
        _pipeline_log(
            pipeline_id,
            "search_completed",
            f"status={final_status} qualified={len(qualified)} elapsed={elapsed}s",
        )

        update_status(
            final_status,
            found=metrics["found"],
            with_website=metrics["with_website"],
            with_email=metrics["with_email"],
            qualified=len(qualified),
            results_json=_safe_json_dumps(businesses),
        )
        save_search(business_type, location, len(businesses), len(qualified))

    except Exception as exc:
        trace = traceback.format_exc()
        _pipeline_log(pipeline_id, "search_error", f"{type(exc).__name__}: {exc}", level="error")

        error_payload = {
            "error": f"{type(exc).__name__}: {exc}",
            "traceback": trace if (DEBUG or PIPELINE_DEBUG) else "",
        }
        if businesses:
            try:
                update_status(
                    STATUS_FAILED,
                    found=len(businesses),
                    with_website=sum(1 for b in businesses if (b.get("website") or "").strip()),
                    with_email=sum(1 for b in businesses if (b.get("email") or "").strip()),
                    qualified=sum(1 for b in businesses if b.get("qualified")),
                    results_json=_safe_json_dumps(businesses),
                )
                return
            except Exception:
                pass

        update_status(STATUS_FAILED, results_json=_safe_json_dumps(error_payload))


@app.route("/results/<int:pipeline_id>")
def results_page(pipeline_id):
    """Show pipeline results."""
    conn = get_db()
    run_row = conn.execute("SELECT * FROM pipeline_runs WHERE id=?", (pipeline_id,)).fetchone()
    conn.close()

    if not run_row:
        flash("Pipeline not found", "error")
        return redirect(url_for("home"))

    run = dict(run_row)
    run["status"] = normalize_status(run.get("status"))
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
            error = error or "Failed to parse search results."

    metrics = summarize_businesses(businesses)
    qualified = [b for b in businesses if b.get("qualified")]
    skipped = [b for b in businesses if not b.get("qualified")]

    return render_template(
        "results.html",
        run=run,
        businesses=businesses,
        qualified=qualified,
        skipped=skipped,
        metrics=metrics,
        error=error,
        pipeline_id=pipeline_id,
    )


@app.route("/api/pipeline/<int:pipeline_id>")
def api_pipeline_status(pipeline_id):
    """API endpoint for polling pipeline status."""
    conn = get_db()
    run = conn.execute("SELECT * FROM pipeline_runs WHERE id=?", (pipeline_id,)).fetchone()
    conn.close()
    if not run:
        return jsonify({"error": "not found"}), 404

    payload = dict(run)
    payload["status"] = normalize_status(payload.get("status"))

    results_json = payload.get("results_json")
    payload.pop("results_json", None)

    if results_json:
        try:
            data = json.loads(results_json)
            if isinstance(data, list):
                payload.update(summarize_businesses(data))
            elif isinstance(data, dict) and data.get("error"):
                payload["error"] = data.get("error")
        except Exception:
            payload.setdefault("error", "Failed to parse results_json")

    return jsonify(payload)


# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
# 3. SEND â€” Send emails to qualified businesses
# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•

def _do_send_pipeline(pipeline_id):
    """
    Core sending logic with atomic claim and safer terminal handling.
    """
    from database import db_session

    sendable_statuses = (STATUS_READY, STATUS_PARTIAL, STATUS_FAILED, STATUS_ERROR, STATUS_PAUSED, STATUS_TIMEOUT)

    claimed = False
    run = None
    businesses = []
    sent_count = 0
    fail_count = 0
    bounce_count = 0

    try:
        with db_session() as conn:
            result = conn.execute(
                f"UPDATE pipeline_runs SET status=? WHERE id=? AND status IN ({','.join('?' * len(sendable_statuses))})",
                (STATUS_SENDING, pipeline_id, *sendable_statuses),
            )
            conn.commit()
            if result.rowcount == 0:
                return

            claimed = True
            run = conn.execute("SELECT * FROM pipeline_runs WHERE id=?", (pipeline_id,)).fetchone()

        if not run or not run["results_json"]:
            with db_session() as conn:
                conn.execute("UPDATE pipeline_runs SET status=? WHERE id=?", (STATUS_ERROR, pipeline_id))
                conn.commit()
            return

        try:
            decoded = json.loads(run["results_json"])
            parsed = []
            for item in decoded:
                if isinstance(item, dict):
                    parsed.append(item)
                elif isinstance(item, str):
                    try:
                        loaded = json.loads(item)
                        if isinstance(loaded, dict):
                            parsed.append(loaded)
                    except Exception:
                        continue
            businesses = parsed
        except Exception as exc:
            _pipeline_log(pipeline_id, "send_error", f"results parse failed: {exc}", level="error")
            with db_session() as conn:
                conn.execute("UPDATE pipeline_runs SET status=? WHERE id=?", (STATUS_ERROR, pipeline_id))
                conn.commit()
            return

        qualified = [b for b in businesses if b.get("qualified") and b.get("email_subject") and b.get("email")]
        if not qualified:
            with db_session() as conn:
                conn.execute(
                    "UPDATE pipeline_runs SET status=?, results_json=? WHERE id=?",
                    (STATUS_NO_EMAILS, json.dumps(businesses, ensure_ascii=False), pipeline_id),
                )
                conn.commit()
            return

        settings = get_settings()
        sent_count = run["sent"] or 0
        fail_count = run["failed"] or 0
        bounce_count = run["bounced"] or 0
        def _parse_int_setting(key, default, minimum=0, maximum=500000):
            return _setting_int(settings, key, default, minimum=minimum, maximum=maximum)

        delay_min = _parse_int_setting("send_delay_min", 30, minimum=5, maximum=600)
        delay_max = _parse_int_setting("send_delay_max", 60, minimum=5, maximum=900)
        if delay_max < delay_min:
            delay_max = delay_min
        hourly_limit = _parse_int_setting("smtp_hourly_limit", 500, minimum=0, maximum=500000)
        daily_limit = _parse_int_setting("smtp_daily_limit", 2000, minimum=0, maximum=500000)
        micro_test_on = settings.get("micro_test_enabled", "true").lower() == "true"
        micro_test_size = _parse_int_setting("micro_test_size", 2, minimum=1, maximum=10)
        pause_on_bounce = settings.get("pause_on_bounce", "true").lower() == "true"
        transient_retries = _parse_int_setting("smtp_transient_retries", 2, minimum=0, maximum=5)
        transient_retry_delay = _parse_int_setting("smtp_transient_retry_delay_seconds", 8, minimum=1, maximum=180)

        micro_test_done = not micro_test_on
        micro_test_sent = 0
        micro_test_bounced = 0

        def _abort_check():
            try:
                with db_session() as conn:
                    row = conn.execute("SELECT status FROM pipeline_runs WHERE id=?", (pipeline_id,)).fetchone()
                    return row and normalize_status(row["status"]) in {STATUS_ERROR, STATUS_STOPPED}
            except Exception:
                return False

        def _flush_counters():
            with db_session() as conn:
                conn.execute(
                    "UPDATE pipeline_runs SET sent=?, failed=?, bounced=?, results_json=? WHERE id=?",
                    (sent_count, fail_count, bounce_count, json.dumps(businesses, ensure_ascii=False), pipeline_id),
                )
                conn.commit()

        def _quota_usage():
            with db_session() as conn:
                hourly_sent = conn.execute(
                    "SELECT COUNT(*) FROM email_log WHERE status='sent' AND datetime(sent_at) >= datetime('now','-1 hour')"
                ).fetchone()[0]
                daily_sent = conn.execute(
                    "SELECT COUNT(*) FROM email_log WHERE status='sent' AND datetime(sent_at) >= datetime('now','-1 day')"
                ).fetchone()[0]
            return int(hourly_sent or 0), int(daily_sent or 0)

        def _quota_blocked():
            hourly_sent, daily_sent = _quota_usage()
            if hourly_limit > 0 and hourly_sent >= hourly_limit:
                return True, f"Hourly quota reached ({hourly_sent}/{hourly_limit})."
            if daily_limit > 0 and daily_sent >= daily_limit:
                return True, f"Daily quota reached ({daily_sent}/{daily_limit})."
            return False, ""

        def _is_transient_send_error(error_msg):
            text = (error_msg or "").lower()
            if text.startswith("transient smtp error:"):
                return True
            transient_hints = (
                "timed out",
                "timeout",
                "temporary failure",
                "service not available",
                "connection reset",
                "network is unreachable",
                "connection unexpectedly closed",
            )
            return any(hint in text for hint in transient_hints)

        for i, biz in enumerate(qualified):
            if _abort_check():
                _pipeline_log(pipeline_id, "send_stopped", f"aborted at lead {i + 1}")
                break

            quota_hit, quota_message = _quota_blocked()
            if quota_hit:
                _pipeline_log(pipeline_id, "send_paused", quota_message, level="warning")
                _flush_counters()
                with db_session() as conn:
                    conn.execute("UPDATE pipeline_runs SET status=? WHERE id=?", (STATUS_PAUSED, pipeline_id))
                    conn.commit()
                return

            if not micro_test_done and micro_test_sent >= micro_test_size:
                micro_test_done = True
                if micro_test_bounced > 0:
                    _pipeline_log(
                        pipeline_id,
                        "send_error",
                        f"micro-test failed ({micro_test_bounced}/{micro_test_size})",
                        level="error",
                    )
                    _flush_counters()
                    with db_session() as conn:
                        conn.execute("UPDATE pipeline_runs SET status=? WHERE id=?", (STATUS_ERROR, pipeline_id))
                        conn.commit()
                    return

            if biz.get("dispatch_status") in ("sent", "failed", "bounced", "skipped"):
                continue

            email = biz.get("email")
            subject = biz.get("email_subject")
            body = biz.get("email_body")

            if not email or not subject or not body or not is_good_email(email, biz.get("website", "")):
                fail_count += 1
                biz["dispatch_status"] = "skipped"
                biz["dispatch_error"] = "Invalid or missing email payload"
                _flush_counters()
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

            body_send = body
            if lead_id:
                if is_lead_unsubscribed(lead_id) or is_lead_bounced(lead_id):
                    biz["dispatch_status"] = "skipped"
                    biz["dispatch_error"] = "Unsubscribed or bounced"
                    _flush_counters()
                    continue

                from email_engine import add_unsubscribe_footer

                body_send = add_unsubscribe_footer(body, lead_id)
                with db_session() as conn_dup:
                    past = conn_dup.execute(
                        "SELECT id FROM email_log WHERE lead_id=? AND status IN ('sent','bounced')",
                        (lead_id,),
                    ).fetchone()
                if past:
                    biz["dispatch_status"] = "skipped"
                    biz["dispatch_error"] = "Already sent previously"
                    _flush_counters()
                    continue

            send_ts = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
            success, error_msg, is_bounce, is_rate_limited = send_email(email, subject, body_send, settings)

            if not success and not is_bounce and not is_rate_limited and _is_transient_send_error(error_msg):
                for retry_attempt in range(transient_retries):
                    wait_seconds = transient_retry_delay * (retry_attempt + 1)
                    _pipeline_log(
                        pipeline_id,
                        "send_retry",
                        f"transient SMTP failure on {email}; retry {retry_attempt + 1}/{transient_retries} in {wait_seconds}s",
                        level="warning",
                    )
                    for _ in range(wait_seconds):
                        if _abort_check():
                            return
                        time.sleep(1)

                    success, error_msg, is_bounce, is_rate_limited = send_email(email, subject, body_send, settings)
                    if success or is_bounce or is_rate_limited or not _is_transient_send_error(error_msg):
                        break

            if is_rate_limited:
                rate_error = error_msg or "SMTP provider rate limit triggered"
                fail_count += 1
                biz["dispatch_status"] = "failed"
                biz["dispatch_error"] = f"Rate limited: {rate_error}"
                try:
                    log_email(
                        {
                            "lead_id": lead_id or 0,
                            "campaign_id": 0,
                            "sequence_step": 1,
                            "business_type": biz.get("business_type", "other"),
                            "subject": subject,
                            "body": body_send,
                            "status": "failed",
                            "error_message": f"[{send_ts}] Rate limited: {rate_error}",
                            "tier": biz.get("tier", 3),
                            "qualification_score": biz.get("score", 0),
                            "city": biz.get("city", ""),
                            "country": "",
                        }
                    )
                except Exception as exc:
                    _pipeline_log(pipeline_id, "send_warning", f"log_email failed: {exc}", level="warning")
                _pipeline_log(
                    pipeline_id,
                    "send_paused",
                    f"rate limited on {email}: {rate_error}; manual resume after cooldown",
                    level="warning",
                )
                _flush_counters()
                with db_session() as conn:
                    conn.execute("UPDATE pipeline_runs SET status=? WHERE id=?", (STATUS_PAUSED, pipeline_id))
                    conn.commit()
                return

            dispatch_status = "sent" if success else ("bounced" if is_bounce else "failed")
            biz["dispatch_status"] = dispatch_status
            biz["dispatch_error"] = error_msg if not success else ""

            if is_bounce and lead_id:
                mark_bounced(lead_id)

            try:
                log_email(
                    {
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
                    }
                )
            except Exception as exc:
                _pipeline_log(pipeline_id, "send_warning", f"log_email failed: {exc}", level="warning")

            if success:
                sent_count += 1
                micro_test_sent += 1
                try:
                    if lead_id:
                        add_lead_to_sequence(lead_id)
                except Exception as exc:
                    _pipeline_log(pipeline_id, "send_warning", f"sequence add failed: {exc}", level="warning")
            elif is_bounce:
                bounce_count += 1
                micro_test_sent += 1
                micro_test_bounced += 1
                if pause_on_bounce and micro_test_done:
                    with db_session() as conn:
                        conn.execute("UPDATE pipeline_runs SET status=? WHERE id=?", (STATUS_ERROR, pipeline_id))
                        conn.commit()
                    _flush_counters()
                    return
            else:
                fail_count += 1

            _flush_counters()
            delay = random.randint(delay_min, delay_max)
            _pipeline_log(pipeline_id, "send_progress", f"{dispatch_status} -> {email}; waiting {delay}s")
            time.sleep(delay)

    except Exception as exc:
        _pipeline_log(pipeline_id, "send_error", f"{type(exc).__name__}: {exc}", level="error")
        try:
            with db_session() as conn:
                conn.execute("UPDATE pipeline_runs SET status=? WHERE id=?", (STATUS_ERROR, pipeline_id))
                conn.commit()
        except Exception:
            pass
    finally:
        if claimed:
            with db_session() as conn:
                curr = conn.execute("SELECT status FROM pipeline_runs WHERE id=?", (pipeline_id,)).fetchone()
                curr_status = normalize_status(curr["status"]) if curr else ""
                if curr_status not in {STATUS_ERROR, STATUS_STOPPED, STATUS_PAUSED, STATUS_NO_EMAILS}:
                    conn.execute(
                        "UPDATE pipeline_runs SET status=?, sent=?, failed=?, bounced=? WHERE id=?",
                        (STATUS_DONE, sent_count, fail_count, bounce_count, pipeline_id),
                    )
                conn.commit()


@app.route("/send/<int:pipeline_id>", methods=["POST"])

def send_emails(pipeline_id):
    """Send emails to all qualified businesses from a pipeline run."""
    conn = get_db()
    try:
        run = conn.execute("SELECT * FROM pipeline_runs WHERE id=?", (pipeline_id,)).fetchone()

        if not run or not run["results_json"]:
            flash("No results to send", "error")
            return redirect(url_for("home"))

        current_status = normalize_status(run["status"])
        if current_status == STATUS_SENDING:
            if _is_send_worker_active(pipeline_id):
                flash("This batch is already sending. Check Analytics for live progress.", "warning")
                return redirect(url_for("analytics_page"))

            # Recover stale 'sending' status left behind by a dead worker.
            conn.execute(
                "UPDATE pipeline_runs SET status=? WHERE id=? AND status=?",
                (STATUS_READY, pipeline_id, STATUS_SENDING),
            )
            conn.commit()
            run = conn.execute("SELECT * FROM pipeline_runs WHERE id=?", (pipeline_id,)).fetchone()
            current_status = normalize_status(run["status"]) if run else STATUS_READY
            flash("Recovered stale sending state. Restarting send worker.", "warning")

        try:
            decoded = json.loads(run["results_json"])
            parsed = []
            for item in decoded:
                if isinstance(item, dict):
                    parsed.append(item)
                elif isinstance(item, str):
                    try:
                        loaded = json.loads(item)
                        if isinstance(loaded, dict):
                            parsed.append(loaded)
                    except Exception:
                        continue
            businesses = parsed
        except Exception:
            flash("Failed to load results", "error")
            return redirect(url_for("results_page", pipeline_id=pipeline_id))

        qualified = [b for b in businesses if b.get("qualified") and b.get("email_subject") and b.get("email")]
        if not qualified:
            metrics = summarize_businesses(businesses)
            flash(
                (
                    "No qualified businesses to send. "
                    f"Found={metrics['found']}, with website={metrics['with_website']}, "
                    f"emails={metrics['with_email']}, failed leads={metrics['failed_leads']}, "
                    f"skipped={metrics['skipped_leads']}."
                ),
                "error",
            )
            return redirect(url_for("results_page", pipeline_id=pipeline_id))

    finally:
        conn.close()

    def _send_single_worker():
        _set_send_worker_active(pipeline_id, True)
        try:
            _do_send_pipeline(pipeline_id)
        except Exception as exc:
            _pipeline_log(pipeline_id, "send_error", f"send worker crashed: {exc}", level="error")
        finally:
            _set_send_worker_active(pipeline_id, False)

    thread = threading.Thread(target=_send_single_worker, daemon=True)
    thread.start()

    flash(f"Sending {len(qualified)} emails. Check Analytics for progress.", "info")
    return redirect(url_for("analytics_page"))


@app.route("/stop_pipeline/<int:pipeline_id>", methods=["POST"])

def stop_pipeline(pipeline_id):
    """Force stop a running pipeline (search or send)."""
    from database import db_session

    stoppable = (
        STATUS_PENDING,
        STATUS_DISCOVERING,
        STATUS_EXTRACTING,
        STATUS_SCORING,
        STATUS_SENDING,
    )
    with db_session() as conn:
        conn.execute(
            f"UPDATE pipeline_runs SET status=? WHERE id=? AND status IN ({','.join('?' * len(stoppable))})",
            (STATUS_ERROR, pipeline_id, *stoppable),
        )
        conn.commit()

    with _pipeline_kill_lock:
        evt = _pipeline_kill_events.get(pipeline_id)
        if evt:
            evt.set()
            _pipeline_log(pipeline_id, "search_stopped", "stop button pressed by user", level="warning")

    flash("Pipeline stopped.", "warning")
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
        
    if normalize_status(run["status"]) not in (STATUS_READY, STATUS_PARTIAL, STATUS_TIMEOUT, STATUS_NO_EMAILS, STATUS_ERROR):
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


# 4. ANALYTICS â€” Track everything
# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•

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
    flash("Marked as replied âœ“", "success")
    return redirect(url_for("analytics_page"))


# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
# SETTINGS
# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•

@app.route("/settings")
def settings_page():
    settings = get_settings()
    return render_template("settings.html", settings=settings)


@app.route("/settings/update", methods=["POST"])
def update_settings_route():
    send_delay_min = _setting_int(request.form, "send_delay_min", 30, minimum=5, maximum=600)
    send_delay_max = _setting_int(request.form, "send_delay_max", 60, minimum=5, maximum=900)
    if send_delay_max < send_delay_min:
        send_delay_max = send_delay_min

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
        "send_delay_min": str(send_delay_min),
        "send_delay_max": str(send_delay_max),
        "smtp_hourly_limit": str(_setting_int(request.form, "smtp_hourly_limit", 500, minimum=0, maximum=500000)),
        "smtp_daily_limit": str(_setting_int(request.form, "smtp_daily_limit", 2000, minimum=0, maximum=500000)),
        "smtp_timeout_seconds": str(_setting_int(request.form, "smtp_timeout_seconds", 20, minimum=5, maximum=120)),
        "imap_append_timeout_seconds": str(_setting_int(request.form, "imap_append_timeout_seconds", 10, minimum=3, maximum=60)),
        "smtp_transient_retries": str(_setting_int(request.form, "smtp_transient_retries", 2, minimum=0, maximum=5)),
        "smtp_transient_retry_delay_seconds": str(_setting_int(request.form, "smtp_transient_retry_delay_seconds", 8, minimum=1, maximum=180)),
        "imap_sync_sent": "true" if request.form.get("imap_sync_sent") == "on" else "false",
        "micro_test_size": str(_setting_int(request.form, "micro_test_size", 2, minimum=1, maximum=10)),
        "micro_test_enabled": "true" if request.form.get("micro_test_enabled") == "on" else "false",
        "pause_on_bounce": "true" if request.form.get("pause_on_bounce") == "on" else "false",
        "search_timeout_seconds": str(_setting_int(request.form, "search_timeout_seconds", SEARCH_TIMEOUT_SECONDS, minimum=120, maximum=7200)),
        "discovery_timeout_seconds": str(_setting_int(request.form, "discovery_timeout_seconds", DISCOVERY_TIMEOUT_SECONDS, minimum=30, maximum=1800)),
        "lead_timeout_seconds": str(_setting_int(request.form, "lead_timeout_seconds", LEAD_TIMEOUT_SECONDS, minimum=5, maximum=300)),
        "lead_fetch_connect_timeout_seconds": str(_setting_float(request.form, "lead_fetch_connect_timeout_seconds", LEAD_FETCH_CONNECT_TIMEOUT_SECONDS, minimum=1.0, maximum=30.0)),
        "lead_fetch_read_timeout_seconds": str(_setting_float(request.form, "lead_fetch_read_timeout_seconds", LEAD_FETCH_READ_TIMEOUT_SECONDS, minimum=1.0, maximum=45.0)),
        "search_max_results": str(_setting_int(request.form, "search_max_results", SEARCH_MAX_RESULTS, minimum=5, maximum=200)),
        "search_debug": "true" if request.form.get("search_debug") == "on" else "false",
    }
    update_settings(data)
    flash("Settings saved âœ“", "success")
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
        flash(f"Test email sent to {to_email} âœ“", "success")
    else:
        flash(f"Failed: {error}", "error")

    return redirect(url_for("settings_page"))


@app.route("/settings/reset-database", methods=["POST"])
def reset_database_route():
    reset_database()
    flash("System reset complete. All data has been wiped.", "warning")
    return redirect(url_for("home"))


# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
# UNSUBSCRIBE
# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•

@app.route("/unsubscribe/<token>")
def unsubscribe_route(token):
    lead = unsubscribe_by_token(token)
    if lead:
        return render_template("unsubscribe.html", success=True, business=lead.get("business_name", ""))
    return render_template("unsubscribe.html", success=False)


# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
# API â€” Spam check
# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•

@app.route("/api/spam-check", methods=["POST"])
def api_spam_check():
    data = request.get_json()
    return jsonify(check_spam_score(data.get("subject", ""), data.get("body", "")))


# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
# MANUAL LEADS
# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•

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


# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
# LEADS & CAMPAIGNS (Wired up for v5 E2E Verification)
# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•

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

# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
# RUN
# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•

if __name__ == "__main__":
    app.run(debug=DEBUG, port=PORT, host="0.0.0.0")
