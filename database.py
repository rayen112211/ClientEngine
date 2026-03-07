"""
SQLite database layer for the Cold Email System.
All tables, queries, and data access in one place.
"""
import sqlite3
import json
import csv
import io
import hashlib
from datetime import datetime, timedelta
from config import DB_PATH, UNSUBSCRIBE_SECRET


def get_db():
    """Get a database connection with row factory."""
    conn = sqlite3.connect(DB_PATH, timeout=30.0)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA busy_timeout=30000")
    conn.execute("PRAGMA cache_size=-64000")  # 64MB cache
    conn.execute("PRAGMA foreign_keys=ON")
    return conn

from contextlib import contextmanager

@contextmanager
def db_session():
    """Context manager for safe database connections.
    Always closes the connection and rolls back on exceptions
    to avoid long-lived SQLite write locks.
    """
    conn = get_db()
    try:
        yield conn
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def dict_from_row(row):
    """Convert sqlite3.Row to dict."""
    if row is None:
        return None
    return dict(row)


def rows_to_dicts(rows):
    """Convert list of sqlite3.Row to list of dicts."""
    return [dict(r) for r in rows]


# ═══════════════════════════════════════════════════════════
# SCHEMA
# ═══════════════════════════════════════════════════════════

def init_db():
    """Create all tables if they don't exist."""
    conn = get_db()
    cursor = conn.cursor()

    cursor.executescript("""
        -- ─── Leads ───────────────────────────────────────────
        CREATE TABLE IF NOT EXISTS leads (
            id INTEGER PRIMARY KEY AUTOINCREMENT,

            -- Core info
            business_name TEXT NOT NULL,
            email TEXT,
            phone TEXT,
            website TEXT,
            category TEXT,
            city TEXT,
            country TEXT DEFAULT 'Italy',
            address TEXT,

            -- Business indicators
            google_rating REAL DEFAULT 0,
            review_count INTEGER DEFAULT 0,
            running_ads INTEGER DEFAULT 0,
            area_type TEXT DEFAULT 'standard',
            year_opened INTEGER,
            competition_level TEXT DEFAULT 'medium',

            -- Enrichment results
            business_type TEXT,
            website_status TEXT DEFAULT 'unchecked',
            website_response_time REAL,
            has_mobile_responsive INTEGER DEFAULT 0,
            has_cta INTEGER DEFAULT 0,

            -- Qualification
            qualification_score INTEGER DEFAULT 0,
            tier INTEGER DEFAULT 3,
            pain_points TEXT DEFAULT '[]',

            -- Management
            status TEXT DEFAULT 'new',
            unsubscribed INTEGER DEFAULT 0,
            bounced INTEGER DEFAULT 0,
            unsubscribe_token TEXT,
            email_source TEXT,
            enriched_at TIMESTAMP,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            notes TEXT
        );

        -- ─── Campaigns ──────────────────────────────────────
        CREATE TABLE IF NOT EXISTS campaigns (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            from_name TEXT DEFAULT 'Rayen',
            from_email TEXT,
            portfolio_link TEXT DEFAULT 'https://rayenlazizi.tech',
            min_score INTEGER DEFAULT 60,
            target_business_types TEXT DEFAULT '["all"]',
            target_tiers TEXT DEFAULT '[1,2,3]',
            delay_min_seconds INTEGER DEFAULT 60,
            delay_max_seconds INTEGER DEFAULT 180,
            status TEXT DEFAULT 'draft',
            total_leads INTEGER DEFAULT 0,
            total_sent INTEGER DEFAULT 0,
            total_replied INTEGER DEFAULT 0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        -- ─── Sequence Tracker ────────────────────────────────
        CREATE TABLE IF NOT EXISTS sequence_tracker (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            lead_id INTEGER NOT NULL,
            campaign_id INTEGER NOT NULL,
            current_step INTEGER DEFAULT 0,
            last_sent_at TIMESTAMP,
            next_send_at TIMESTAMP,
            status TEXT DEFAULT 'pending',
            replied INTEGER DEFAULT 0,
            FOREIGN KEY (lead_id) REFERENCES leads(id),
            FOREIGN KEY (campaign_id) REFERENCES campaigns(id),
            UNIQUE(lead_id, campaign_id)
        );

        -- ─── Email Log ───────────────────────────────────────
        CREATE TABLE IF NOT EXISTS email_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            lead_id INTEGER NOT NULL,
            campaign_id INTEGER NOT NULL,
            sequence_step INTEGER NOT NULL,
            business_type TEXT,
            subject TEXT,
            body TEXT,
            status TEXT DEFAULT 'sent',
            sent_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            replied INTEGER DEFAULT 0,
            replied_at TIMESTAMP,
            error_message TEXT,
            tier INTEGER,
            qualification_score INTEGER,
            city TEXT,
            country TEXT,
            FOREIGN KEY (lead_id) REFERENCES leads(id),
            FOREIGN KEY (campaign_id) REFERENCES campaigns(id)
        );

        -- ─── Settings ────────────────────────────────────────
        CREATE TABLE IF NOT EXISTS settings (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL
        );

        -- ─── Search History ──────────────────────────────────
        CREATE TABLE IF NOT EXISTS search_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            business_type TEXT,
            location TEXT,
            results_count INTEGER DEFAULT 0,
            qualified_count INTEGER DEFAULT 0,
            leads_imported INTEGER DEFAULT 0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        -- ─── Pipeline Runs ──────────────────────────────────────
        CREATE TABLE IF NOT EXISTS pipeline_runs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            query TEXT NOT NULL,
            location TEXT,
            status TEXT DEFAULT 'pending',
            found INTEGER DEFAULT 0,
            with_website INTEGER DEFAULT 0,
            with_email INTEGER DEFAULT 0,
            qualified INTEGER DEFAULT 0,
            sent INTEGER DEFAULT 0,
            failed INTEGER DEFAULT 0,
            bounced INTEGER DEFAULT 0,
            results_json TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            completed_at TIMESTAMP
        );

        -- ─── Manual Leads (No Website / Social Only) ────────
        CREATE TABLE IF NOT EXISTS manual_leads (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            pipeline_id INTEGER,
            business_name TEXT NOT NULL,
            social_link TEXT,
            phone TEXT,
            city TEXT,
            category TEXT,
            google_rating REAL,
            review_count INTEGER,
            status TEXT DEFAULT 'new',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        -- ─── Search Queue (Batch) ────────────────────────────
        CREATE TABLE IF NOT EXISTS search_queue (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            query TEXT NOT NULL,
            location TEXT DEFAULT '',
            status TEXT DEFAULT 'pending',
            pipeline_id INTEGER,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            started_at TIMESTAMP,
            completed_at TIMESTAMP
        );
    """)

    # Add new columns to existing tables (safe migration)
    migrations = [
        # --- Original v4 migrations ---
        "ALTER TABLE leads ADD COLUMN unsubscribed INTEGER DEFAULT 0",
        "ALTER TABLE leads ADD COLUMN bounced INTEGER DEFAULT 0",
        "ALTER TABLE leads ADD COLUMN unsubscribe_token TEXT",
        "ALTER TABLE leads ADD COLUMN email_source TEXT",
        # --- v5: Multi-source tracking ---
        "ALTER TABLE leads ADD COLUMN source TEXT DEFAULT 'google_maps'",
        "ALTER TABLE leads ADD COLUMN instagram_url TEXT",
        "ALTER TABLE leads ADD COLUMN facebook_url TEXT",
        "ALTER TABLE leads ADD COLUMN whatsapp TEXT",
        # --- v5: Website quality ---
        "ALTER TABLE leads ADD COLUMN website_score INTEGER DEFAULT 0",
        "ALTER TABLE leads ADD COLUMN has_ssl INTEGER DEFAULT 0",
        "ALTER TABLE leads ADD COLUMN has_mobile INTEGER DEFAULT 0",
        "ALTER TABLE leads ADD COLUMN cms_detected TEXT",
        "ALTER TABLE leads ADD COLUMN has_contact_form INTEGER DEFAULT 0",
        "ALTER TABLE leads ADD COLUMN page_speed_ms INTEGER",
        # --- v5: Tier + Channel ---
        "ALTER TABLE leads ADD COLUMN tier INTEGER DEFAULT 3",
        "ALTER TABLE leads ADD COLUMN outreach_channel TEXT DEFAULT 'email'",
        "ALTER TABLE leads ADD COLUMN is_new_business INTEGER DEFAULT 0",
        # --- v5: manual_leads source tracking ---
        "ALTER TABLE manual_leads ADD COLUMN source TEXT DEFAULT 'google_maps'",
        "ALTER TABLE manual_leads ADD COLUMN instagram_url TEXT",
        "ALTER TABLE manual_leads ADD COLUMN facebook_url TEXT",
        "ALTER TABLE manual_leads ADD COLUMN whatsapp TEXT",
        # --- v5.1: Pipeline/Queue Source choice ---
        "ALTER TABLE search_queue ADD COLUMN source_choice TEXT DEFAULT 'all'",
        "ALTER TABLE pipeline_runs ADD COLUMN source_choice TEXT DEFAULT 'all'",
    ]
    for migration in migrations:
        try:
            cursor.execute(migration)
        except sqlite3.OperationalError:
            pass  # Column already exists

    # Clean up orphaned pipeline runs from previous server crashes/restarts
    # Any in-progress status means the server died mid-run — mark them error
    cursor.execute("""
        UPDATE pipeline_runs SET status='error'
        WHERE status IN ('sending', 'running', 'pending', 'searching', 'discovering', 'extracting', 'scoring', 'filtering')
    """)

    # Ensure default campaign (ID=0) exists for pipeline direct-sends
    try:
        cursor.execute("INSERT OR IGNORE INTO campaigns (id, name, from_name, from_email, target_business_types, target_tiers) VALUES (0, 'Direct Pipeline Sends', 'System', '', '[\"all\"]', '[1,2,3]')")
    except sqlite3.OperationalError:
        pass

    # Default settings
    defaults = {
        "smtp_host": "smtp.gmail.com",
        "smtp_port": "465",
        "smtp_user": "",
        "smtp_password": "",
        "smtp_use_ssl": "true",
        "from_name": "Rayen",
        "from_email": "",
        "portfolio_link": "https://rayenlazizi.tech",
        "google_places_api_key": "",
        "search_timeout_seconds": "900",
        "discovery_timeout_seconds": "180",
        "lead_timeout_seconds": "45",
        "lead_fetch_connect_timeout_seconds": "4",
        "lead_fetch_read_timeout_seconds": "7",
        "search_max_results": "80",
        "search_debug": "false",
    }
    for key, value in defaults.items():
        cursor.execute(
            "INSERT OR IGNORE INTO settings (key, value) VALUES (?, ?)",
            (key, value),
        )

    conn.commit()
    conn.close()


# ═══════════════════════════════════════════════════════════
# MANUAL LEADS (No Website)
# ═══════════════════════════════════════════════════════════

def save_manual_lead(pipeline_id, biz):
    """Save a business with no website or only social links for manual outreach."""
    conn = get_db()
    conn.execute(
        """
        INSERT INTO manual_leads
        (pipeline_id, business_name, social_link, phone, city, category, google_rating, review_count,
         source, instagram_url, facebook_url, whatsapp)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            pipeline_id,
            biz.get("business_name", "Unknown"),
            biz.get("website", ""),  # the social link
            biz.get("phone", ""),
            biz.get("city", ""),
            biz.get("category", ""),
            biz.get("google_rating", 0.0),
            biz.get("review_count", 0),
            biz.get("source", "google_maps"),
            biz.get("instagram_url", ""),
            biz.get("facebook_url", ""),
            biz.get("whatsapp", ""),
        ),
    )
    conn.commit()
    conn.close()


def get_manual_leads(status="new", source=None):
    """Get manual leads by status (new, contacted, ignored) and optional source."""
    conn = get_db()
    if source and source != "all":
        leads = conn.execute(
            "SELECT * FROM manual_leads WHERE status = ? AND source = ? ORDER BY id DESC", (status, source)
        ).fetchall()
    else:
        leads = conn.execute(
            "SELECT * FROM manual_leads WHERE status = ? ORDER BY id DESC", (status,)
        ).fetchall()
    conn.close()
    return rows_to_dicts(leads)


def mark_manual_lead_status(lead_id, status):
    """Update status of a manual lead."""
    conn = get_db()
    conn.execute("UPDATE manual_leads SET status = ? WHERE id = ?", (status, lead_id))
    conn.commit()
    conn.close()


# ═══════════════════════════════════════════════════════════
# LEADS CRUD
# ═══════════════════════════════════════════════════════════

def add_lead(data):
    """Insert a single lead. Returns the new lead id."""
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO leads (
            business_name, email, phone, website, category,
            city, country, address, google_rating, review_count,
            running_ads, area_type, year_opened, competition_level, notes,
            source, instagram_url, facebook_url, whatsapp,
            website_score, has_ssl, has_mobile, cms_detected, has_contact_form, page_speed_ms,
            tier, outreach_channel, is_new_business
        ) VALUES (
            ?, ?, ?, ?, ?,
            ?, ?, ?, ?, ?,
            ?, ?, ?, ?, ?,
            ?, ?, ?, ?,
            ?, ?, ?, ?, ?, ?,
            ?, ?, ?
        )
    """, (
        data.get("business_name", ""),
        data.get("email", ""),
        data.get("phone", ""),
        data.get("website", ""),
        data.get("category", ""),
        data.get("city", ""),
        data.get("country", ""),
        data.get("address", ""),
        data.get("google_rating", 0.0),
        data.get("review_count", 0),
        1 if data.get("running_ads") else 0,
        data.get("area_type", "standard"),
        data.get("year_opened"),
        data.get("competition_level", "medium"),
        data.get("notes", ""),
        data.get("source", "google_maps"),
        data.get("instagram_url", ""),
        data.get("facebook_url", ""),
        data.get("whatsapp", ""),
        data.get("website_check", {}).get("website_score", 0) if isinstance(data.get("website_check"), dict) else data.get("website_score", 0),
        1 if (data.get("website_check", {}).get("has_ssl") if isinstance(data.get("website_check"), dict) else data.get("has_ssl")) else 0,
        1 if (data.get("website_check", {}).get("has_mobile") if isinstance(data.get("website_check"), dict) else data.get("has_mobile")) else 0,
        data.get("website_check", {}).get("cms_detected") if isinstance(data.get("website_check"), dict) else data.get("cms_detected", ""),
        1 if (data.get("website_check", {}).get("has_contact_form") if isinstance(data.get("website_check"), dict) else data.get("has_contact_form")) else 0,
        data.get("website_check", {}).get("response_time_ms", 0) if isinstance(data.get("website_check"), dict) else data.get("page_speed_ms", 0),
        data.get("tier", 3),
        data.get("outreach_channel", "email"),
        1 if data.get("is_new_business") else 0,
    ))
    lead_id = cursor.lastrowid
    conn.commit()
    conn.close()
    return lead_id


def import_leads_csv(csv_text):
    """Import leads from CSV text. Returns (imported_count, skipped_count)."""
    reader = csv.DictReader(io.StringIO(csv_text))
    imported = 0
    skipped = 0
    conn = get_db()
    cursor = conn.cursor()

    for row in reader:
        # Normalize keys to lowercase
        row = {k.strip().lower().replace(" ", "_"): v.strip() for k, v in row.items()}

        bname = row.get("business_name", row.get("name", row.get("business", "")))
        email = row.get("email", "")

        if not bname:
            skipped += 1
            continue

        # Check duplicate by email (if email exists)
        if email:
            existing = cursor.execute(
                "SELECT id FROM leads WHERE email = ?", (email,)
            ).fetchone()
            if existing:
                skipped += 1
                continue

        cursor.execute("""
            INSERT INTO leads (
                business_name, email, phone, website, category,
                city, country, address, google_rating, review_count,
                running_ads, area_type, year_opened, competition_level
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            bname,
            email,
            row.get("phone", ""),
            row.get("website", row.get("url", "")),
            row.get("category", row.get("type", "")),
            row.get("city", ""),
            row.get("country", "Italy"),
            row.get("address", ""),
            float(row.get("google_rating", row.get("rating", 0)) or 0),
            int(row.get("review_count", row.get("reviews", 0)) or 0),
            1 if row.get("running_ads", "").lower() in ("true", "1", "yes", "si") else 0,
            row.get("area_type", "standard"),
            int(row["year_opened"]) if row.get("year_opened") else None,
            row.get("competition_level", "medium"),
        ))
        imported += 1

    conn.commit()
    conn.close()
    return imported, skipped


def get_leads(status=None, min_score=None, tier=None, business_type=None, exclude_unsubscribed=False, exclude_bounced=False, limit=500):
    """Get leads with optional filters."""
    conn = get_db()
    query = "SELECT * FROM leads WHERE 1=1"
    params = []

    if status:
        query += " AND status = ?"
        params.append(status)
    if min_score is not None:
        query += " AND qualification_score >= ?"
        params.append(min_score)
    if tier is not None:
        query += " AND tier = ?"
        params.append(tier)
    if business_type:
        query += " AND business_type = ?"
        params.append(business_type)
    if exclude_unsubscribed:
        query += " AND (unsubscribed = 0 OR unsubscribed IS NULL)"
    if exclude_bounced:
        query += " AND (bounced = 0 OR bounced IS NULL)"

    query += " ORDER BY qualification_score DESC, created_at DESC LIMIT ?"
    params.append(limit)

    rows = conn.execute(query, params).fetchall()
    conn.close()
    return rows_to_dicts(rows)


def get_lead(lead_id):
    """Get a single lead by ID."""
    conn = get_db()
    row = conn.execute("SELECT * FROM leads WHERE id = ?", (lead_id,)).fetchone()
    conn.close()
    return dict_from_row(row)


def update_lead(lead_id, data):
    """Update lead fields."""
    conn = get_db()
    fields = []
    values = []
    for key, value in data.items():
        fields.append(f"{key} = ?")
        values.append(value)
    fields.append("updated_at = ?")
    values.append(datetime.utcnow().isoformat())
    values.append(lead_id)

    conn.execute(
        f"UPDATE leads SET {', '.join(fields)} WHERE id = ?",
        values,
    )
    conn.commit()
    conn.close()


def delete_lead(lead_id):
    """Delete a lead and its tracking data."""
    conn = get_db()
    conn.execute("DELETE FROM sequence_tracker WHERE lead_id = ?", (lead_id,))
    conn.execute("DELETE FROM email_log WHERE lead_id = ?", (lead_id,))
    conn.execute("DELETE FROM leads WHERE id = ?", (lead_id,))
    conn.commit()
    conn.close()


def get_leads_count():
    """Get total leads and breakdown by status."""
    conn = get_db()
    total = conn.execute("SELECT COUNT(*) FROM leads").fetchone()[0]
    by_status = rows_to_dicts(conn.execute(
        "SELECT status, COUNT(*) as count FROM leads GROUP BY status"
    ).fetchall())
    by_tier = rows_to_dicts(conn.execute(
        "SELECT tier, COUNT(*) as count FROM leads WHERE qualification_score > 0 GROUP BY tier"
    ).fetchall())
    by_type = rows_to_dicts(conn.execute(
        "SELECT business_type, COUNT(*) as count FROM leads WHERE business_type IS NOT NULL GROUP BY business_type"
    ).fetchall())
    conn.close()
    return {
        "total": total,
        "by_status": {r["status"]: r["count"] for r in by_status},
        "by_tier": {r["tier"]: r["count"] for r in by_tier},
        "by_type": {r["business_type"]: r["count"] for r in by_type},
    }


# ═══════════════════════════════════════════════════════════
# CAMPAIGNS CRUD
# ═══════════════════════════════════════════════════════════

def create_campaign(data):
    """Create a new campaign."""
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO campaigns (
            name, from_name, from_email, portfolio_link,
            min_score, target_business_types, target_tiers,
            delay_min_seconds, delay_max_seconds
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        data.get("name", "Campaign"),
        data.get("from_name", "Rayen"),
        data.get("from_email", ""),
        data.get("portfolio_link", "https://rayenlazizi.tech"),
        int(data.get("min_score", 60)),
        json.dumps(data.get("target_business_types", ["all"])),
        json.dumps(data.get("target_tiers", [1, 2, 3])),
        int(data.get("delay_min_seconds", 60)),
        int(data.get("delay_max_seconds", 180)),
    ))
    campaign_id = cursor.lastrowid
    conn.commit()
    conn.close()
    return campaign_id


def get_campaigns():
    """Get all campaigns."""
    conn = get_db()
    rows = conn.execute(
        "SELECT * FROM campaigns ORDER BY created_at DESC"
    ).fetchall()
    conn.close()
    return rows_to_dicts(rows)


def get_campaign(campaign_id):
    """Get a single campaign."""
    conn = get_db()
    row = conn.execute(
        "SELECT * FROM campaigns WHERE id = ?", (campaign_id,)
    ).fetchone()
    conn.close()
    return dict_from_row(row)


def update_campaign(campaign_id, data):
    """Update campaign fields."""
    conn = get_db()
    fields = []
    values = []
    for key, value in data.items():
        fields.append(f"{key} = ?")
        values.append(value)
    fields.append("updated_at = ?")
    values.append(datetime.utcnow().isoformat())
    values.append(campaign_id)

    conn.execute(
        f"UPDATE campaigns SET {', '.join(fields)} WHERE id = ?",
        values,
    )
    conn.commit()
    conn.close()


# ═══════════════════════════════════════════════════════════
# SEQUENCE TRACKER
# ═══════════════════════════════════════════════════════════

def enroll_leads_in_campaign(campaign_id):
    """
    Enroll all qualified leads into a campaign's sequence.
    Returns count of newly enrolled leads.
    """
    campaign = get_campaign(campaign_id)
    if not campaign:
        return 0

    min_score = campaign["min_score"]
    target_types = json.loads(campaign["target_business_types"])
    target_tiers = json.loads(campaign["target_tiers"])

    conn = get_db()

    query = """
        SELECT l.id FROM leads l
        WHERE l.qualification_score >= ?
          AND l.email IS NOT NULL AND l.email != ''
          AND l.status IN ('enriched', 'active')
          AND l.id NOT IN (
              SELECT lead_id FROM sequence_tracker WHERE campaign_id = ?
          )
    """
    params = [min_score, campaign_id]

    if "all" not in target_types:
        placeholders = ",".join("?" * len(target_types))
        query += f" AND l.business_type IN ({placeholders})"
        params.extend(target_types)

    if target_tiers:
        placeholders = ",".join("?" * len(target_tiers))
        query += f" AND l.tier IN ({placeholders})"
        params.extend(target_tiers)

    leads = conn.execute(query, params).fetchall()

    now = datetime.utcnow().isoformat()
    enrolled = 0
    for lead in leads:
        conn.execute("""
            INSERT OR IGNORE INTO sequence_tracker
            (lead_id, campaign_id, current_step, next_send_at, status)
            VALUES (?, ?, 0, ?, 'pending')
        """, (lead["id"], campaign_id, now))
        enrolled += 1

    # Update campaign total
    conn.execute(
        "UPDATE campaigns SET total_leads = total_leads + ?, status = 'active', updated_at = ? WHERE id = ?",
        (enrolled, now, campaign_id),
    )

    conn.commit()
    conn.close()
    return enrolled


def add_lead_to_sequence(lead_id):
    """
    Directly add a single lead to the sequence tracker for automated follow-ups.
    Uses campaign_id 0 (default pipeline campaign).
    """
    conn = get_db()
    
    # Calculate next send time (3 days for step 1 -> step 2)
    now = datetime.utcnow()
    next_send = (now + timedelta(days=3)).isoformat()
    
    conn.execute("""
        INSERT OR IGNORE INTO sequence_tracker
        (lead_id, campaign_id, current_step, last_sent_at, next_send_at, status, replied)
        VALUES (?, 0, 1, ?, ?, 'active', 0)
    """, (lead_id, now.isoformat(), next_send))
    
    conn.commit()
    conn.close()


def get_due_sequences(campaign_id):
    """Get all sequence entries that are due for their next email."""
    conn = get_db()
    now = datetime.utcnow().isoformat()
    rows = conn.execute("""
        SELECT st.*, l.business_name, l.email, l.website, l.category,
               l.city, l.country, l.business_type, l.qualification_score,
               l.tier, l.pain_points, l.google_rating, l.review_count
        FROM sequence_tracker st
        JOIN leads l ON st.lead_id = l.id
        WHERE st.campaign_id = ?
          AND st.status IN ('pending', 'active')
          AND st.current_step < 5
          AND st.next_send_at <= ?
          AND st.replied = 0
        ORDER BY l.tier ASC, l.qualification_score DESC
    """, (campaign_id, now)).fetchall()
    conn.close()
    return rows_to_dicts(rows)


def advance_sequence(tracker_id, next_delay_days):
    """Move a sequence entry to the next step."""
    conn = get_db()
    now = datetime.utcnow()
    next_send = (now + timedelta(days=next_delay_days)).isoformat()

    tracker = conn.execute(
        "SELECT * FROM sequence_tracker WHERE id = ?", (tracker_id,)
    ).fetchone()

    new_step = tracker["current_step"] + 1
    new_status = "active" if new_step < 5 else "completed"

    conn.execute("""
        UPDATE sequence_tracker
        SET current_step = ?, last_sent_at = ?, next_send_at = ?, status = ?
        WHERE id = ?
    """, (new_step, now.isoformat(), next_send, new_status, tracker_id))

    conn.commit()
    conn.close()


def mark_replied(tracker_id):
    """Mark a sequence as replied (stops further emails)."""
    conn = get_db()
    now = datetime.utcnow().isoformat()
    conn.execute("""
        UPDATE sequence_tracker SET replied = 1, status = 'replied' WHERE id = ?
    """, (tracker_id,))

    # Also update the campaign replied count
    tracker = conn.execute(
        "SELECT campaign_id FROM sequence_tracker WHERE id = ?", (tracker_id,)
    ).fetchone()
    if tracker:
        conn.execute(
            "UPDATE campaigns SET total_replied = total_replied + 1 WHERE id = ?",
            (tracker["campaign_id"],),
        )

    conn.commit()
    conn.close()


# ═══════════════════════════════════════════════════════════
# EMAIL LOG
# ═══════════════════════════════════════════════════════════

def log_email(data):
    """Log a sent email."""
    conn = get_db()
    conn.execute("""
        INSERT INTO email_log (
            lead_id, campaign_id, sequence_step, business_type,
            subject, body, status, sent_at, error_message,
            tier, qualification_score, city, country
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        data["lead_id"],
        data["campaign_id"],
        data["sequence_step"],
        data.get("business_type"),
        data.get("subject"),
        data.get("body"),
        data.get("status", "sent"),
        datetime.utcnow().isoformat(),
        data.get("error_message"),
        data.get("tier"),
        data.get("qualification_score"),
        data.get("city"),
        data.get("country"),
    ))

    # Update campaign sent count
    if data.get("status") == "sent":
        conn.execute(
            "UPDATE campaigns SET total_sent = total_sent + 1, updated_at = ? WHERE id = ?",
            (datetime.utcnow().isoformat(), data["campaign_id"]),
        )

    conn.commit()
    conn.close()


def get_email_log(campaign_id=None, limit=200):
    """Get email log entries."""
    conn = get_db()
    if campaign_id:
        rows = conn.execute(
            "SELECT * FROM email_log WHERE campaign_id = ? ORDER BY sent_at DESC LIMIT ?",
            (campaign_id, limit),
        ).fetchall()
    else:
        rows = conn.execute(
            "SELECT * FROM email_log ORDER BY sent_at DESC LIMIT ?",
            (limit,),
        ).fetchall()
    conn.close()
    return rows_to_dicts(rows)

def already_sent(lead_id, sequence_step):
    """Check if a specific sequence step was already sent to this lead."""
    conn = get_db()
    row = conn.execute(
        "SELECT 1 FROM email_log WHERE lead_id = ? AND sequence_step = ? AND status = 'sent'",
        (lead_id, sequence_step)
    ).fetchone()
    conn.close()
    return bool(row)


# ═══════════════════════════════════════════════════════════
# ANALYTICS
# ═══════════════════════════════════════════════════════════

def get_analytics():
    """Get comprehensive analytics data."""
    conn = get_db()

    # Total emails
    total_sent = conn.execute(
        "SELECT COUNT(*) FROM email_log WHERE status = 'sent'"
    ).fetchone()[0]
    total_failed = conn.execute(
        "SELECT COUNT(*) FROM email_log WHERE status = 'failed'"
    ).fetchone()[0]
    total_replied = conn.execute(
        "SELECT COUNT(*) FROM email_log WHERE replied = 1"
    ).fetchone()[0]

    # By business type
    by_type = rows_to_dicts(conn.execute("""
        SELECT business_type,
               COUNT(*) as sent,
               SUM(CASE WHEN replied = 1 THEN 1 ELSE 0 END) as replies
        FROM email_log WHERE status = 'sent' AND business_type IS NOT NULL
        GROUP BY business_type
    """).fetchall())

    # By sequence step
    by_step = rows_to_dicts(conn.execute("""
        SELECT sequence_step,
               COUNT(*) as sent,
               SUM(CASE WHEN replied = 1 THEN 1 ELSE 0 END) as replies
        FROM email_log WHERE status = 'sent'
        GROUP BY sequence_step ORDER BY sequence_step
    """).fetchall())

    # By tier
    by_tier = rows_to_dicts(conn.execute("""
        SELECT tier,
               COUNT(*) as sent,
               SUM(CASE WHEN replied = 1 THEN 1 ELSE 0 END) as replies
        FROM email_log WHERE status = 'sent' AND tier IS NOT NULL
        GROUP BY tier ORDER BY tier
    """).fetchall())

    # By city
    by_city = rows_to_dicts(conn.execute("""
        SELECT city,
               COUNT(*) as sent,
               SUM(CASE WHEN replied = 1 THEN 1 ELSE 0 END) as replies
        FROM email_log WHERE status = 'sent' AND city IS NOT NULL AND city != ''
        GROUP BY city ORDER BY sent DESC LIMIT 20
    """).fetchall())

    # By score range
    by_score = rows_to_dicts(conn.execute("""
        SELECT
            CASE
                WHEN qualification_score >= 90 THEN '90-100'
                WHEN qualification_score >= 75 THEN '75-89'
                WHEN qualification_score >= 60 THEN '60-74'
                ELSE 'Below 60'
            END as score_range,
            COUNT(*) as sent,
            SUM(CASE WHEN replied = 1 THEN 1 ELSE 0 END) as replies
        FROM email_log WHERE status = 'sent'
        GROUP BY score_range ORDER BY score_range DESC
    """).fetchall())

    # Recent activity (last 30 days)
    thirty_days_ago = (datetime.utcnow() - timedelta(days=30)).isoformat()
    daily_activity = rows_to_dicts(conn.execute("""
        SELECT DATE(sent_at) as date,
               COUNT(*) as sent,
               SUM(CASE WHEN replied = 1 THEN 1 ELSE 0 END) as replies
        FROM email_log WHERE status = 'sent' AND sent_at >= ?
        GROUP BY DATE(sent_at) ORDER BY date
    """, (thirty_days_ago,)).fetchall())

    # Top subjects
    top_subjects = rows_to_dicts(conn.execute("""
        SELECT subject,
               COUNT(*) as sent,
               SUM(CASE WHEN replied = 1 THEN 1 ELSE 0 END) as replies
        FROM email_log WHERE status = 'sent'
        GROUP BY subject ORDER BY replies DESC, sent DESC LIMIT 10
    """).fetchall())

    conn.close()

    reply_rate = round((total_replied / total_sent * 100), 1) if total_sent > 0 else 0

    return {
        "total_sent": total_sent,
        "total_failed": total_failed,
        "total_replied": total_replied,
        "reply_rate": reply_rate,
        "by_type": by_type,
        "by_step": by_step,
        "by_tier": by_tier,
        "by_city": by_city,
        "by_score": by_score,
        "daily_activity": daily_activity,
        "top_subjects": top_subjects,
    }


# ═══════════════════════════════════════════════════════════
# SETTINGS
# ═══════════════════════════════════════════════════════════

def get_settings():
    """Get all settings as a dict."""
    conn = get_db()
    rows = conn.execute("SELECT key, value FROM settings").fetchall()
    conn.close()
    return {r["key"]: r["value"] for r in rows}


def update_settings(data):
    """Update multiple settings."""
    conn = get_db()
    for key, value in data.items():
        conn.execute(
            "INSERT OR REPLACE INTO settings (key, value) VALUES (?, ?)",
            (key, str(value)),
        )
    conn.commit()
    conn.close()


# ═══════════════════════════════════════════════════════════
# UNSUBSCRIBE & BOUNCE
# ═══════════════════════════════════════════════════════════

def generate_unsubscribe_token(lead_id):
    """Generate a unique unsubscribe token for a lead."""
    raw = f"{lead_id}-{UNSUBSCRIBE_SECRET}"
    token = hashlib.sha256(raw.encode()).hexdigest()[:24]
    conn = get_db()
    conn.execute("UPDATE leads SET unsubscribe_token = ? WHERE id = ?", (token, lead_id))
    conn.commit()
    conn.close()
    return token


def get_unsubscribe_token(lead_id):
    """Get or create an unsubscribe token for a lead."""
    conn = get_db()
    row = conn.execute("SELECT unsubscribe_token FROM leads WHERE id = ?", (lead_id,)).fetchone()
    conn.close()
    if row and row["unsubscribe_token"]:
        return row["unsubscribe_token"]
    return generate_unsubscribe_token(lead_id)


def unsubscribe_by_token(token):
    """Unsubscribe a lead by token. Returns lead info or None."""
    conn = get_db()
    lead = conn.execute("SELECT id, business_name FROM leads WHERE unsubscribe_token = ?", (token,)).fetchone()
    if lead:
        conn.execute("UPDATE leads SET unsubscribed = 1, status = 'unsubscribed' WHERE id = ?", (lead["id"],))
        conn.execute("UPDATE sequence_tracker SET status = 'unsubscribed' WHERE lead_id = ?", (lead["id"],))
        conn.commit()
        conn.close()
        return dict_from_row(lead)
    conn.close()
    return None


def is_lead_unsubscribed(lead_id):
    """Check if a lead has unsubscribed."""
    conn = get_db()
    row = conn.execute("SELECT unsubscribed FROM leads WHERE id = ?", (lead_id,)).fetchone()
    conn.close()
    return bool(row and row["unsubscribed"])


def mark_bounced(lead_id):
    """Mark a lead as bounced (bad email)."""
    conn = get_db()
    conn.execute("UPDATE leads SET bounced = 1, status = 'bounced' WHERE id = ?", (lead_id,))
    conn.execute("UPDATE sequence_tracker SET status = 'bounced' WHERE lead_id = ?", (lead_id,))
    conn.commit()
    conn.close()


def is_lead_bounced(lead_id):
    """Check if a lead has bounced."""
    conn = get_db()
    row = conn.execute("SELECT bounced FROM leads WHERE id = ?", (lead_id,)).fetchone()
    conn.close()
    return bool(row and row["bounced"])


# ═══════════════════════════════════════════════════════════
# SEARCH HISTORY
# ═══════════════════════════════════════════════════════════

def save_search(business_type, location, results_count, qualified_count=0, leads_imported=0):
    """Log a search in history."""
    conn = get_db()
    conn.execute("""
        INSERT INTO search_history (business_type, location, results_count, qualified_count, leads_imported)
        VALUES (?, ?, ?, ?, ?)
    """, (business_type, location, results_count, qualified_count, leads_imported))
    conn.commit()
    conn.close()


def get_search_history(limit=20):
    """Get recent search history."""
    conn = get_db()
    rows = conn.execute(
        "SELECT * FROM search_history ORDER BY created_at DESC LIMIT ?", (limit,)
    ).fetchall()
    conn.close()
    return rows_to_dicts(rows)


# ═══════════════════════════════════════════════════════════
# DATA MANAGEMENT / DELETION
# ═══════════════════════════════════════════════════════════

def delete_pipeline_run(pipeline_id):
    """Delete a specific pipeline run."""
    conn = get_db()
    conn.execute("DELETE FROM pipeline_runs WHERE id = ?", (pipeline_id,))
    conn.commit()
    conn.close()

def reset_database():
    """Nuclear option: Delete all data to start fresh, keeps settings."""
    conn = get_db()
    tables = [
        "sequence_tracker",
        "email_log",
        "manual_leads",
        "leads",
        "search_history",
        "pipeline_runs",
        "search_queue",
    ]
    
    for table in tables:
        conn.execute(f"DELETE FROM {table}")
        
    # Delete campaigns except default (ID 0)
    conn.execute("DELETE FROM campaigns WHERE id != 0")
    
    # Reset auto increment sequences
    conn.execute(
        "DELETE FROM sqlite_sequence WHERE name IN (?, ?, ?, ?, ?, ?, ?, ?)",
        ("sequence_tracker", "email_log", "manual_leads", "leads", "search_history", "pipeline_runs", "campaigns", "search_queue")
    )
    
    conn.commit()
    conn.close()


# ═══════════════════════════════════════════════════════════
# SEARCH QUEUE (Batch)
# ═══════════════════════════════════════════════════════════

def enqueue_searches(queries, source_choice="all"):
    """Add a list of (query, location) tuples to the batch queue."""
    conn = get_db()
    
    # Auto-cleanup old queue if currently empty/idle to restart progress bar visually
    active = conn.execute("SELECT COUNT(*) FROM search_queue WHERE status IN ('pending', 'running')").fetchone()[0]
    if active == 0:
        conn.execute("DELETE FROM search_queue WHERE status IN ('done', 'error')")
        
    for query, location in queries:
        conn.execute(
            "INSERT INTO search_queue (query, location, source_choice, status) VALUES (?, ?, ?, 'pending')",
            (query, location, source_choice)
        )
    conn.commit()
    conn.close()


def dequeue_next_search():
    """Get and atomically lock the next pending queue item. Returns dict or None."""
    with db_session() as conn:
        conn.execute("BEGIN IMMEDIATE")
        row = conn.execute(
            "SELECT * FROM search_queue WHERE status='pending' ORDER BY id ASC LIMIT 1"
        ).fetchone()
        if not row:
            conn.commit()
            return None

        updated = conn.execute(
            "UPDATE search_queue SET status='running', started_at=? WHERE id=? AND status='pending'",
            (datetime.utcnow().isoformat(), row["id"]),
        )
        conn.commit()

        if updated.rowcount == 0:
            return None

        row = conn.execute("SELECT * FROM search_queue WHERE id=?", (row["id"],)).fetchone()
        return dict(row) if row else None


def complete_queue_item(queue_id, pipeline_id, error=False):
    """Mark a queue item as done or errored."""
    conn = get_db()
    conn.execute(
        "UPDATE search_queue SET status=?, pipeline_id=?, completed_at=? WHERE id=?",
        ("error" if error else "done", pipeline_id, datetime.utcnow().isoformat(), queue_id)
    )
    conn.commit()
    conn.close()


def clear_search_queue():
    """Remove all items from the search queue."""
    conn = get_db()
    conn.execute("DELETE FROM search_queue")
    conn.commit()
    conn.close()

def get_queue_status():
    """Return counts of pending, running, done items."""
    conn = get_db()
    pending = conn.execute("SELECT COUNT(*) FROM search_queue WHERE status='pending'").fetchone()[0]
    running = conn.execute("SELECT COUNT(*) FROM search_queue WHERE status='running'").fetchone()[0]
    done = conn.execute("SELECT COUNT(*) FROM search_queue WHERE status='done'").fetchone()[0]
    error = conn.execute("SELECT COUNT(*) FROM search_queue WHERE status='error'").fetchone()[0]
    total = conn.execute("SELECT COUNT(*) FROM search_queue").fetchone()[0]
    recent = conn.execute(
        "SELECT * FROM search_queue ORDER BY id DESC LIMIT 20"
    ).fetchall()
    conn.close()
    return {
        "pending": pending,
        "running": running,
        "done": done,
        "error": error,
        "total": total,
        "recent": rows_to_dicts(recent),
    }

# ═══════════════════════════════════════════════════════════
# BULK RESET FUNCTIONS
# ═══════════════════════════════════════════════════════════

def clear_all_pipeline_runs():
    """Wipes all search history and pipeline runs from the dashboard."""
    conn = get_db()
    conn.execute("DELETE FROM pipeline_runs")
    conn.execute("DELETE FROM search_history")
    conn.execute("DELETE FROM search_queue")
    conn.execute("DELETE FROM sqlite_sequence WHERE name IN ('pipeline_runs', 'search_history', 'search_queue')")
    conn.commit()
    conn.close()

def cancel_all_sequences():
    """Wipes the sequence tracker to instantly stop all automated follow-up emails."""
    conn = get_db()
    conn.execute("DELETE FROM sequence_tracker")
    conn.execute("DELETE FROM sqlite_sequence WHERE name='sequence_tracker'")
    
    # Reset campaign counts since we stopped tracking
    conn.execute("UPDATE campaigns SET total_leads = 0, total_sent = 0, total_replied = 0")
    
    conn.commit()
    conn.close()

def clear_all_manual_leads():
    """Wipes all manual leads from the database."""
    conn = get_db()
    conn.execute("DELETE FROM manual_leads")
    conn.execute("DELETE FROM sqlite_sequence WHERE name='manual_leads'")
    conn.commit()
    conn.close()

