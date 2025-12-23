# api/admin/weekly.py

from fastapi import APIRouter, Depends

from data.permissions import ensure_internal_allowed
from data.chart_week import close_tracking_week, open_new_tracking_week
from data.region_store import lock_region
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

    Safe guarantees:
    - Idempotent region locking
    - Safe week rotation
    - Auditable via index.json
    """

    # 1️⃣ Lock all regions (idempotent)
    published_regions = []
    for region in ("Eastern", "Northern", "Western"):
        lock_region(region)
        published_regions.append(region)

    # 2️⃣ Close current tracking week (safe if already closed)
    close_tracking_week()

    # 3️⃣ Open a new tracking week
    week = open_new_tracking_week()

    # 4️⃣ Record scheduler run (Cloudflare cron)
    scheduler_state = record_scheduler_run(
        trigger="cloudflare_worker"
    )

    return {
        "status": "ok",
        "published_regions": published_regions,
        "week": week,
        "scheduler": scheduler_state,
    }