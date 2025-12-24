# api/admin/weekly.py

from fastapi import APIRouter, Depends

from data.permissions import ensure_internal_allowed
from data.chart_week import (
    current_chart_week,
    close_tracking_week,
    open_new_tracking_week,
)
from data.region_store import lock_region
from data.region_snapshots import save_region_snapshot
from data.scheduler_state import record_scheduler_run
from data.index import record_week_publish

router = APIRouter()

REGIONS = ("Eastern", "Northern", "Western")


@router.post(
    "/weekly-run",
    summary="(Internal) Weekly publish, snapshot regions, rotate chart week",
)
def run_weekly(
    _: None = Depends(ensure_internal_allowed),
):
    """
    Cloudflare-cron-safe weekly rotation.

    Guarantees:
    - Snapshots written for CURRENT week
    - Regions locked
    - Week index recorded (immutable)
    - New week opened
    """

    # 0️⃣ Resolve current week (SOURCE OF TRUTH)
    current_week = current_chart_week() or {}
    week_id = current_week.get("week_id", "untracked-week")

    published = []

    # 1️⃣ Snapshot + lock regions (CURRENT WEEK)
    for region in REGIONS:
        save_region_snapshot(region)
        lock_region(region)
        published.append(region)

    # 2️⃣ Record immutable index entry (CURRENT WEEK)
    index_entry = record_week_publish(
        week_id=week_id,
        regions=published,
        trigger="cloudflare_worker",
    )

    # 3️⃣ Close current week
    close_tracking_week()

    # 4️⃣ Open new tracking week
    new_week = open_new_tracking_week()

    # 5️⃣ Record scheduler run
    scheduler = record_scheduler_run(
        trigger="cloudflare_worker"
    )

    return {
        "status": "ok",
        "published_regions": published,
        "published_week": week_id,
        "new_week": new_week,
        "scheduler": scheduler,
        "index": index_entry,
    }