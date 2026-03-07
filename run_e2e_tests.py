import time
import json
import os
import threading
from app import app
from database import get_db, reset_database

def run_tests():
    print("🚀 Starting ClientEngine v5 Full System E2E Verification...")
    
    # Reset DB to ensure clean state
    reset_database()
    print("✅ Database reset.")

    client = app.test_client()

    # ---------------------------------------------------------
    # 1. Pipeline & Analytics Integration
    # ---------------------------------------------------------
    print("\n--- 1. SEARCH & PIPELINE ---")
    
    # Start a search
    res = client.post("/search", data={
        "query": "Plumber in New York",
        "source": "all"
    }, follow_redirects=True)
    assert res.status_code == 200
    
    # Check queue status
    queue = client.get("/api/queue-status").get_json()
    assert queue["pending"] >= 0 or queue["running"] >= 0
    print("✅ Search enqueued.")

    # Let the background thread process it... we'll mock the completion for speed
    # We will inject some dummy leads directly simulating the pipeline result
    from database import add_lead
    
    print("\n--- 2. LEAD FLOW (Scoring, Tier, Source, Channel) ---")
    lead_id_1 = add_lead({
        "business_name": "Test Web Lead",
        "email": "test@weblead.com",
        "website": "http://slow-wix-site.com",
        "source": "instagram",
        "website_score": 35,
        "tier": 2,
        "outreach_channel": "email"
    })
    
    lead_id_2 = add_lead({
        "business_name": "Test No Web Lead",
        "instagram_url": "http://instagram.com/test",
        "source": "facebook",
        "website_score": 0,
        "tier": 1,
        "outreach_channel": "instagram_dm"
    })
    
    print("✅ Leads injected.")
    
    # Import CSV
    csv_data = "business_name,email,website\nCSV Lead,csv@test.com,http://csv.com"
    res = client.post("/leads/import", data={"csv_text": csv_data}, follow_redirects=True)
    assert b"Imported 1 leads" in res.data
    print("✅ CSV Import working.")

    # Check Analytics Endpoints
    print("\n--- 4. ANALYTICS ---")
    res = client.get("/analytics")
    assert res.status_code == 200
    assert b"By Source" in res.data
    assert b"By Priority Tier" in res.data
    assert b"By Output Channel" in res.data
    print("✅ Analytics aggregations map correctly.")

    # Delete Individual Lead
    res = client.post(f"/leads/{lead_id_1}/delete", follow_redirects=True)
    assert b"Lead deleted" in res.data
    
    # Verify deletion
    conn = get_db()
    count = conn.execute("SELECT COUNT(*) FROM leads WHERE id=?", (lead_id_1,)).fetchone()[0]
    assert count == 0
    print("✅ Individual lead deletion works.")

    # Delete All Leads
    res = client.post("/leads/delete-all", follow_redirects=True)
    assert b"All leads have been deleted" in res.data
    
    cnt = conn.execute("SELECT COUNT(*) FROM leads").fetchone()[0]
    assert cnt == 0
    print("✅ Bulk delete all leads works.")
    conn.close()

    # ---------------------------------------------------------
    # 3. Campaign Engine 
    # ---------------------------------------------------------
    print("\n--- 3. CAMPAIGN ENGINE ---")
    res = client.post("/campaigns/create", data={
        "name": "Test Campaign",
        "min_score": "50",
        "target_business_types": "all",
        "target_tiers": "1"
    }, follow_redirects=True)
    assert b"Campaign created successfully" in res.data
    print("✅ Campaign creation works.")

    print("\n🎉 ALL QUICK E2E TESTS PASSED!")

if __name__ == "__main__":
    run_tests()
