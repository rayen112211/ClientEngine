"""
Background Daemon for Automated Follow-ups (Sequences)

This script runs in a continuous loop alongside the main web server.
It wakes up periodically to check the `sequence_tracker` table.
If it finds a lead that is due for their next follow-up message (e.g. Day 3, Day 7),
it generates the correct email template, sends it, and schedules the next one.
"""
import time
import json
from datetime import datetime, timedelta
import threading

from database import get_db, mark_bounced, add_lead_to_sequence, get_settings, get_unsubscribe_token, already_sent
from email_engine import send_email, personalize, add_unsubscribe_footer
from templates_data import get_template

# Schedule gaps between emails:
# Email 1 -> (wait 3 days) -> Email 2
# Email 2 -> (wait 4 days) -> Email 3 (total 7 days)
# Email 3 -> (wait 7 days) -> Email 4 (total 14 days)
# Email 4 -> (wait 7 days) -> Email 5 (total 21 days)
STEP_DELAYS_DAYS = {
    1: 3, # Wait 3 days after step 1
    2: 4, # Wait 4 days after step 2
    3: 7, # Wait 7 days after step 3
    4: 7, # Wait 7 days after step 4
}

def process_followups():
    """Check the database and send any followups that are due."""
    print("[Automated Follow-ups] Waking up to check sequences...")
    conn = get_db()
    
    # Get leads due for their next message
    now = datetime.utcnow().isoformat()
    
    # We only process if they haven't replied, bounced, or unsubscribed.
    # The leads table tracks 'bounced' and 'unsubscribed' flags.
    # The sequence_tracker tracks 'replied' flag (which is updated via the Analytics UI).
    query = """
        SELECT
            st.id as tracker_id,
            st.current_step,
            l.id as lead_id,
            l.email,
            l.business_name,
            l.business_type,
            l.city,
            l.website
        FROM sequence_tracker st
        JOIN leads l ON st.lead_id = l.id
        WHERE st.status = 'active'
          AND st.next_send_at <= ?
          AND st.replied = 0
          AND (l.bounced IS NULL OR l.bounced = 0)
          AND (l.unsubscribed IS NULL OR l.unsubscribed = 0)
    """
    due_leads = conn.execute(query, (now,)).fetchall()
    
    if not due_leads:
        print("[Automated Follow-ups] No follow-ups due right now.")
        conn.close()
        return

    print(f"[Automated Follow-ups] Found {len(due_leads)} emails to send.")
    settings = get_settings()

    for row in due_leads:
        tracker_id = row["tracker_id"]
        lead_id = row["lead_id"]
        current_step = row["current_step"]
        email = row["email"]
        
        next_step = current_step + 1
        
        # Try to atomically lock the row to prevent multiprocess race conditions (e.g., 4 emails at once)
        cursor = conn.execute("UPDATE sequence_tracker SET status = 'processing' WHERE id = ? AND status = 'active'", (tracker_id,))
        if cursor.rowcount == 0:
            continue # Another process or thread already grabbed this lead
        conn.commit()
        
        # Double check already_sent directly from email_log to guarantee 0 duplicates
        if already_sent(lead_id, next_step):
            print(f"[Automated Follow-ups] Step {next_step} already sent to {email}. Advancing sequence.")
            delay_days = STEP_DELAYS_DAYS.get(next_step, 7)
            next_send_time = (datetime.utcnow() + timedelta(days=delay_days)).isoformat()
            
            conn.execute("""
                UPDATE sequence_tracker 
                SET current_step = ?, next_send_at = ?, status = 'active'
                WHERE id = ?
            """, (next_step, next_send_time, tracker_id))
            conn.commit()
            continue

        # Get the template for the next step
        template = get_template(row["business_type"] or "other", next_step, row["city"] or "")
        
        if not template:
            # End of sequence (no more templates)
            conn.execute("UPDATE sequence_tracker SET status = 'completed' WHERE id = ?", (tracker_id,))
            conn.commit()
            print(f"[Automated Follow-ups] Lead {email} finished their sequence (no more templates).")
            continue
            
        # Select random subject variant
        import random
        subject_template = random.choice(template["subject_variants"])
        body_template = template["body"]
        
        # Use central personalize to handle {{VARIABLES}} and {Spintax|Options}
        subject = personalize(subject_template, dict(row), settings)
        body = personalize(body_template, dict(row), settings)
        
        # Unsubscribe link (Standard plaintext footer)
        body = add_unsubscribe_footer(body, lead_id)
        
        # Send Email
        success, error_msg, is_bounce, is_rate_limited = send_email(email, subject, body, settings)
        
        # If rate limited, stop processing follow-ups for now — they'll be retried next cycle
        if is_rate_limited:
            print(f"[Automated Follow-ups] Rate limit hit on {email}. Stopping this cycle — will retry next wakeup.")
            conn.execute("UPDATE sequence_tracker SET status = 'active' WHERE id = ?", (tracker_id,))
            conn.commit()
            conn.close()
            return
        
        status = "sent" if success else ("bounced" if is_bounce else "failed")
        
        # Log it
        conn.execute("""
            INSERT INTO email_log (lead_id, campaign_id, sequence_step, business_type, subject, body, status, error_message)
            VALUES (?, 0, ?, ?, ?, ?, ?, ?)
        """, (lead_id, next_step, row["business_type"], subject, body, status, error_msg))
        
        if is_bounce:
            mark_bounced(lead_id)
            conn.execute("UPDATE sequence_tracker SET status = 'error' WHERE id = ?", (tracker_id,))
        elif success:
            # Schedule next step
            delay_days = STEP_DELAYS_DAYS.get(next_step)
            if delay_days:
                next_send_time = (datetime.utcnow() + timedelta(days=delay_days)).isoformat()
                conn.execute("""
                    UPDATE sequence_tracker 
                    SET current_step = ?, last_sent_at = ?, next_send_at = ?, status = 'active'
                    WHERE id = ?
                """, (next_step, now, next_send_time, tracker_id))
            else:
                # Flow finished
                conn.execute("""
                    UPDATE sequence_tracker 
                    SET current_step = ?, last_sent_at = ?, status = 'completed'
                    WHERE id = ?
                """, (next_step, now, tracker_id))
        else:
            # Transient failure. Revert to 'active' to try again next time we wake up
            conn.execute("UPDATE sequence_tracker SET status = 'active' WHERE id = ?", (tracker_id,))
            
        conn.commit()
        # Be nice to SMTP
        time.sleep(15)
        
    conn.close()

def start_daemon():
    """Start the infinite loop checking for followups."""
    def run_loop():
        # Wait a minute before first run so the server starts fully
        time.sleep(10)
        while True:
            try:
                process_followups()
            except Exception as e:
                print(f"[Automated Follow-ups] CRITICAL ERROR: {e}")
            # Sleep for 1 hour
            time.sleep(3600)
            
    thread = threading.Thread(target=run_loop, daemon=True)
    thread.start()
    print("✅ Automated Follow-up Daemon started (checks every hour).")
