"""
ClientEngine — Cold Email Client Acquisition System
Run this file to start the application.
"""
import sys
import os

def cleanup_zombies():
    """Kill any other running python instances and reset stuck database states."""
    import os
    import subprocess
    import sqlite3
    
    current_pid = os.getpid()
    print(f"🧹 Cleaning up environment (Current PID: {current_pid})...")

    # 1. Kill other python processes (Windows specific)
    try:
        # We use a filter to skip the current process so we don't commit suicide
        subprocess.run(
            f'taskkill /F /IM python.exe /FI "PID ne {current_pid}"',
            shell=True, capture_output=True
        )
        print("✅ Zombie Python processes terminated.")
    except Exception as e:
        print(f"⚠️ Process cleanup warning: {e}")

    # 2. Reset database state
    try:
        db_path = os.path.join("data", "leads.db")
        if os.path.exists(db_path):
            conn = sqlite3.connect(db_path)
            # Reset pipeline runs that were stuck in progress
            stuck_states = ("pending", "discovering", "searching", "extracting", "scoring")
            placeholders = ",".join(["?"] * len(stuck_states))
            conn.execute(
                f"UPDATE pipeline_runs SET status='failed' WHERE status IN ({placeholders})",
                stuck_states
            )
            # Reset search queue
            conn.execute("UPDATE search_queue SET status='pending' WHERE status='running'")
            conn.commit()
            conn.close()
            print("✅ Database states reset to clean slate.")
    except Exception as e:
        print(f"⚠️ Database cleanup warning: {e}")


def main():
    # Ensure we're in the right directory
    os.chdir(os.path.dirname(os.path.abspath(__file__)))

    # Run cleanup
    cleanup_zombies()

    # Initialize database
    from database import init_db
    print("⚡ Initializing database...")
    init_db()

    # Check if .env exists
    if not os.path.exists(".env"):
        print("\n📋 No .env file found. Creating template...")
        with open(".env", "w") as f:
            f.write("""# ClientEngine Configuration
# Copy this file or edit it with your real values

# SMTP Settings (Gmail example)
SMTP_HOST=smtp.gmail.com
SMTP_PORT=465
SMTP_USER=your-email@gmail.com
SMTP_PASSWORD=your-app-password
SMTP_USE_SSL=true

# Sender Info
FROM_NAME=Rayen
FROM_EMAIL=your-email@gmail.com
PORTFOLIO_LINK=https://rayenlazizi.tech

# Email Delays (seconds between each email)
DELAY_MIN_SECONDS=60
DELAY_MAX_SECONDS=180

# Flask
SECRET_KEY=change-this-to-a-random-string
DEBUG=true
PORT=5000
""")
        print("   Created .env — edit it with your SMTP credentials")
        print("   Then run this script again.\n")

    # Start the app
    from app import app
    from config import DEBUG, PORT

    print(f"""
╔══════════════════════════════════════════════════╗
║          ⚡ ClientEngine v4.0                    ║
║     Cold Email Automation — Simple & Smart       ║
╠══════════════════════════════════════════════════╣
║                                                  ║
║  App:  http://localhost:{PORT}                      ║
║                                                  ║
║  Quick Start:                                    ║
║  1. Configure SMTP + API key in Settings         ║
║  2. Search: "Restaurant Barcelona"               ║
║  3. Review & Send                                ║
║  4. Track replies in Analytics                   ║
║                                                  ║
╚══════════════════════════════════════════════════╝
""")

    app.run(debug=DEBUG, port=PORT, host="0.0.0.0")


if __name__ == "__main__":
    main()
