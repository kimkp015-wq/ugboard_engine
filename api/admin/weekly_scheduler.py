# api/admin/weekly_scheduler.py

from data.region_store import lock_region
from data.chart_week import open_new_tracking_week


def run_weekly_scheduler():
    """
    Weekly automation:
    - Lock/publish all regions
    - Open next tracking window
    - Record successful scheduler run

    Safe to call multiple times.
    """

    # 1️⃣ Publish / lock regions
    lock_region("Eastern")
    lock_region("Northern")
    lock_region("Western")

    # 2️⃣ Open next tracking window
    open_new_tracking_week()

    # 3️⃣ Record successful scheduler run
    # (import inside function prevents circular imports)
    from data.scheduler_state import record_scheduler_run
    record_scheduler_run()

    return {
        "published": ["Eastern", "Northern", "Western"],
        "status": "completed",
    }