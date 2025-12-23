# api/admin/weekly_scheduler.py

"""
Weekly automation engine (EAT timezone)

Responsibilities:
- Publish & lock all regional charts
- Open the next tracking week
- Record a successful scheduler run (for alerting)
"""

from datetime import datetime
from zoneinfo import ZoneInfo

from data.scheduler_state import record_scheduler_run
from data.region_store import lock_region, is_region_locked
from data.chart_week import open_new_tracking_week

EAT = ZoneInfo("Africa/Kampala")

REGIONS = ["Eastern", "Northern", "Western"]


def run_weekly_scheduler():
    """
    Run weekly automation.

    Safe guarantees:
    - Idempotent (safe if called twice)
    - No crash on already-locked regions
    - Records success ONLY after full completion
    """

    published = []
    skipped = []

    # 1️⃣ Publish / lock regions (idempotent)
    for region in REGIONS:
        if is_region_locked(region):
            skipped.append(region)
            continue

        lock_region(region)
        published.append(region)

    # 2️⃣ Open next tracking window
    open_new_tracking_week()

    # 3️⃣ Record successful scheduler execution
    record_scheduler_run()

    return {
        "status": "completed",
        "timestamp": datetime.now(EAT).isoformat(),
        "published": published,
        "skipped": skipped,
    }