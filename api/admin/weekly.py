# api/admin/weekly.py

from fastapi import APIRouter, Depends

from data.permissions import ensure_internal_allowed
from data.chart_week import close_tracking_week, open_new_tracking_week
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
    - Snapshots written
    - Regions locked
    - Week rotated
    - Index appended (immutable)
    """

    published = []

    # 1️⃣ Snapshot + lock
    for region in REGIONS:
        save_region_snapshot(region)
        lock_region(region)
        published.append(region)

    # 2️⃣ Close current week
    close_tracking_week()

    # 3️⃣ Open new week
    week = open_new_tracking_week()

    # 4️⃣ Scheduler state
    scheduler = record_scheduler_run(
        trigger="cloudflare_worker"
    )

    # 5️⃣ Index record (SOURCE OF TRUTH)
    index_entry = record_week_publish(
        week_id=week["week_id"],
        regions=published,
        trigger="cloudflare_worker",
    )

    return {
        "status": "ok",
        "published_regions": published,
        "new_week": week,
        "scheduler": scheduler,
        "index": index_entry,
    }