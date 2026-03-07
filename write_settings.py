"""
Standalone script — writes SMTP settings directly to SQLite.
NO imports from app/enrichment/database to avoid DNS/thread hangs.
"""
import sqlite3, os, sys

db_path = os.path.join(os.path.dirname(__file__), "data", "leads.db")
print(f"DB path: {db_path}")

settings = [
    ("smtp_host",          "mail.spacemail.com"),
    ("smtp_port",          "465"),
    ("smtp_use_ssl",       "true"),
    ("smtp_user",          "hello@rayenlazizi.tech"),
    ("smtp_password",      "Rayen@@2003"),
    ("from_name",          "Rayen"),
    ("from_email",         "hello@rayenlazizi.tech"),
    ("reply_to",           "hello@rayenlazizi.tech"),
    ("send_delay_min",     "30"),
    ("send_delay_max",     "60"),
    ("micro_test_enabled", "true"),
    ("micro_test_size",    "2"),
    ("pause_on_bounce",    "true"),
]

conn = sqlite3.connect(db_path, timeout=10)
for k, v in settings:
    conn.execute("INSERT OR REPLACE INTO settings(key,value) VALUES(?,?)", (k, v))
conn.commit()

print("\n✅ Settings saved:")
for k, v in settings:
    row = conn.execute("SELECT value FROM settings WHERE key=?", (k,)).fetchone()
    marker = "✓" if row and row[0] == v else "✗"
    print(f"  [{marker}] {k} = {row[0] if row else 'MISSING'}")

conn.close()
print("\nDone.")
