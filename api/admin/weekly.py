from fastapi import APIRouter, Depends

from data.permissions import ensure_internal_allowed
from data.chart_week import (
    close_tracking_week,
    open_new_tracking_week,
    current_chart_week,
)
from data.region_store import lock_region
from data.region_snapshots import save_region_snapshot
from data.top100_snapshot import save_top100_snapshot
from data.scheduler_state import record_scheduler_run
from data.index import record_week_publish, week_already_published

router = APIRouter()
REGIONS = ("Eastern", "Northern", "Western")


@router.post(
    "/weekly-run",
    summary="(Internal) Weekly publish (idempotent, safe)",
)
def run_weekly(
    _: None = Depends(ensure_internal_allowed),
):
    current_week = current_chart_week()
    week_id = current_week["week_id"]

    # 🛑 IDEMPOTENCY GUARD
    if week_already_published(week_id):
        return {
            "status": "skipped",
            "reason": "week already published",
            "week_id": week_id,
        }

    published = []

    # 1️⃣ Snapshot + lock
    for region in REGIONS:
        save_region_snapshot(region)
        lock_region(region)
        published.append(region)

    # 2️⃣ Close week
    close_tracking_week()

    # 3️⃣ Open new week
    new_week = open_new_tracking_week()

    # 4️⃣ Record scheduler
    scheduler = record_scheduler_run(trigger="cloudflare_worker")

    # 5️⃣ Immutable index write (SOURCE OF TRUTH)
    index = record_week_publish(
        week_id=week_id,
        regions=published,
        trigger="cloudflare_worker",
    )

    return {
        "status": "ok",
        "published_regions": published,
        "closed_week": week_id,
        "new_week": new_week["week_id"],
        "index": index,
        "scheduler": scheduler,
    }