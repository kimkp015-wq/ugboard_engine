# api/admin/weekly.py

from fastapi import APIRouter, Depends

from data.permissions import ensure_internal_allowed
from data.chart_week import close_tracking_week, open_new_tracking_week
from data.region_store import lock_region
from data.region_snapshots import save_region_snapshot
from data.scheduler_state import record_scheduler_run

router = APIRouter()


@router.post(
    "/weekly-run",
    summary="(Internal) Weekly publish and rotate chart week",
)
def run_weekly(
    _: None = Depends(ensure_internal_allowed),
):
    """
    Internal-only endpoint.

    Designed to be called by:
    - Cloudflare Workers cron
    - Trusted internal scheduler

    Guarantees:
    - Snapshots are created BEFORE locking
    - Idempotent region locking
    - Safe week rotation
    - Fully auditable
    """

    published_regions = []

    # 1️⃣ Snapshot + lock all regions
    for region in ("Eastern", "Northern", "Western"):
        save_region_snapshot(region)   # ✅ REQUIRED
        lock_region(region)
        published_regions.append(region)

    # 2️⃣ Close current tracking week
    close_tracking_week()

    # 3️⃣ Open a new tracking week
    week = open_new_tracking_week()

    # 4️⃣ Record scheduler run
    scheduler_state = record_scheduler_run(
        trigger="cloudflare_worker"
    )

    return {
        "status": "ok",
        "published_regions": published_regions,
        "week": week,
        "scheduler": scheduler_state,
    }