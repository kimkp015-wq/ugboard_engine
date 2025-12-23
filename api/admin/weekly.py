from fastapi import APIRouter, Depends
from data.permissions import ensure_admin_allowed
from data.chart_week import open_new_tracking_week
from data.region_store import lock_region
from data.scheduler_state import record_scheduler_run

router = APIRouter()


@router.post(
    "/weekly-run",
    summary="Run weekly publish and open new chart week",
)
def run_weekly(
    _: None = Depends(ensure_admin_allowed),
):
    # Lock all regions
    lock_region("Eastern")
    lock_region("Northern")
    lock_region("Western")

    # Open new tracking week
    week = open_new_tracking_week()

    # Record scheduler run
    record_scheduler_run()

    return {
        "status": "ok",
        "published_regions": ["Eastern", "Northern", "Western"],
        "week": week,
    }