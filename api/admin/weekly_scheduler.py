from data.region_store import lock_region
from data.chart_week import open_new_tracking_week


def run_weekly_scheduler():
    lock_region("Eastern")
    lock_region("Northern")
    lock_region("Western")

    open_new_tracking_week()

    from data.scheduler_state import record_scheduler_run
    record_scheduler_run()

    return {
        "published": ["Eastern", "Northern", "Western"],
        "status": "completed",
    }