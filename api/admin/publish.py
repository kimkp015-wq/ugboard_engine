from fastapi import APIRouter, Depends, HTTPException

from data.permissions import ensure_admin_allowed
from data.region_store import lock_region, is_region_locked
from data.region_snapshots import save_region_snapshot
from data.chart_week import close_tracking_week, open_new_tracking_week
from data.scheduler_state import record_scheduler_run

router = APIRouter()

REGIONS = ("Eastern", "Northern", "Western")


@router.post(
    "/publish/weekly",
    summary="Publish all regions and rotate chart week",
)
def publish_weekly(
    _: None = Depends(ensure_admin_allowed),
):
    published = []

    # 1️⃣ Snapshot + lock each region
    for region in REGIONS:
        if is_region_locked(region):
            continue

        try:
            save_region_snapshot(region)
            lock_region(region)
            published.append(region)
        except Exception as e:
            raise HTTPException(
                status_code=500,
                detail=f"Failed publishing {region}: {str(e)}",
            )

    # 2️⃣ Close current week
    close_tracking_week()

    # 3️⃣ Open new tracking week
    open_new_tracking_week()

    # 4️⃣ Record scheduler/admin run
    record_scheduler_run()

    return {
        "status": "ok",
        "published_regions": published,
        "week_rotated": True,
    }