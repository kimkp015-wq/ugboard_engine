# api/admin/publish.py

from fastapi import APIRouter, Depends, HTTPException

from data.permissions import ensure_admin_allowed
from data.region_store import lock_region, is_region_locked
from data.region_snapshots import save_region_snapshot
from data.chart_week import (
    get_current_week_id,
    close_tracking_week,
    open_new_tracking_week,
)
from data.scheduler_state import record_scheduler_run
from data.index import record_week_publish, week_already_published

router = APIRouter()

REGIONS = ("Eastern", "Northern", "Western")


@router.post(
    "/publish/weekly",
    summary="Publish all regions and rotate chart week",
)
def publish_weekly(
    _: None = Depends(ensure_admin_allowed),
):
    # -------------------------
    # Resolve current week
    # -------------------------
    week_id = get_current_week_id()

    # -------------------------
    # Idempotency guard
    # -------------------------
    if week_already_published(week_id):
        return {
            "status": "skipped",
            "reason": "week already published",
            "week_id": week_id,
        }

    published = []

    # -------------------------
    # Snapshot + lock regions
    # -------------------------
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

    # -------------------------
    # Nothing new → safe exit
    # -------------------------
    if not published:
        return {
            "status": "ok",
            "published_regions": [],
            "week_rotated": False,
            "message": "All regions already published",
            "week_id": week_id,
        }

    # -------------------------
    # Rotate chart week
    # -------------------------
    close_tracking_week()
    open_new_tracking_week()

    # -------------------------
    # Record publish + admin run
    # -------------------------
    record_week_publish(week_id)
    record_scheduler_run()

    return {
        "status": "ok",
        "published_regions": published,
        "week_rotated": True,
        "week_id": week_id,
    }