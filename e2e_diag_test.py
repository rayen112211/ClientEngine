import os
import sqlite3
import time
from database import get_settings, init_db, get_db
from email_engine import send_test_email
from business_discovery import search_businesses
from enrichment import is_good_email
import traceback

def run_diagnostics():
    print("========================================")
    print("      SYSTEM HEALTH & E2E CHECK         ")
    print("========================================\n")

    # 1. Database Check
    print("[1] Database Integrity:")
    try:
        if not os.path.exists("data/leads.db"):
            init_db()
        conn = get_db()
        tables = conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
        if len(tables) > 5:
            print("  ✅ Database tables present and accessible")
        else:
            print("  ❌ Database might be missing tables")
        conn.close()
    except Exception as e:
        print(f"  ❌ DB Error: {e}")

    # 2. Settings Check
    print("\n[2] System Settings:")
    settings = get_settings()
    
    api_key = settings.get("google_places_api_key", "")
    if api_key and len(api_key) > 10:
        print(f"  ✅ Google Places API Key: {api_key[:10]}***")
    else:
        print(f"  ❌ Google Places API Key Missing!")

    smtp_host = settings.get("smtp_host", "")
    smtp_port = str(settings.get("smtp_port", ""))
    if smtp_host and smtp_port:
        print(f"  ✅ SMTP Configured: {smtp_host}:{smtp_port}")
    else:
        print(f"  ❌ SMTP Configuration missing!")

    # 3. Email format check (DNS bug fix validation)
    print("\n[3] Email Engine Validation speed:")
    t0 = time.time()
    try:
        good = is_good_email("info@testbusiness.com")
        bad = is_good_email("noreply@testbusiness.com")
        t_ms = (time.time() - t0) * 1000
        if good and not bad and t_ms < 100:
            print(f"  ✅ Email check fast & accurate ({t_ms:.2f}ms)")
        else:
            print(f"  ❌ Email check slow or inaccurate ({t_ms:.2f}ms)")
    except Exception as e:
         print(f"  ❌ Email check failed: {e}")

    # 4. Search API Mock Check
    print("\n[4] Google Maps API Test:")
    try:
        # Just query 1 result with a small radius logic to check key validity
        import requests
        url = "https://maps.googleapis.com/maps/api/place/textsearch/json"
        params = {"query": "cafe in Rome", "key": api_key}
        resp = requests.get(url, params=params, timeout=5)
        data = resp.json()
        if resp.status_code == 200 and "results" in data:
            print(f"  ✅ Google Places API is active (status: {data.get('status')})")
        else:
            print(f"  ❌ Google Places API failed: {data.get('error_message', 'Unknown error')}")
    except Exception as e:
        print(f"  ❌ Google Maps connection error: {e}")

    # 5. SMTP Send Test
    print("\n[5] SMTP Live Test:")
    try:
        success, error = send_test_email(settings.get("from_email"), settings)
        if success:
            print(f"  ✅ SpaceMail SMTP test email sent successfully to {settings.get('from_email')}")
        else:
            print(f"  ❌ SpaceMail SMTP failed: {error}")
    except Exception as e:
        print(f"  ❌ Traceback Error: {e}")
        traceback.print_exc()

    print("\n========================================")
    print("      DIAGNOSTIC COMPLETE               ")
    print("========================================")

if __name__ == "__main__":
    run_diagnostics()
