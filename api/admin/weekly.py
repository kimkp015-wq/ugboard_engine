from fastapi import APIRouter, Depends

from data.permissions import ensure_internal_allowed
from data.chart_week import close_tracking_week, open_new_tracking_week
from data.region_store import lock_region
from data.scheduler_state import record_scheduler_run

router = APIRouter()


@router.post(
    "/weekly-run",
    summary="(Internal) Weekly publish + rotate chart week",
)
def run_weekly(
    _: None = Depends(ensure_internal_allowed),
):
    # 1️⃣ Lock all regions
    lock_region("Eastern")
    lock_region("Northern")
    lock_region("Western")

    # 2️⃣ Close current tracking week (if any)
    close_tracking_week()

    # 3️⃣ Open new tracking week
    week = open_new_tracking_week()

    # 4️⃣ Record scheduler run
    record_scheduler_run()

    return {
        "status": "ok",
        "published_regions": ["Eastern", "Northern", "Western"],
        "week": week,
        "trigger": "internal_scheduler",
    }