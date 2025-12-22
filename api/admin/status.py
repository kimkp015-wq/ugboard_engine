from fastapi import APIRouter
from datetime import datetime

from data.store import load_items
from data.region_store import load_region_locks
from data.region_snapshots import load_region_snapshot

router = APIRouter()

REGIONS = ["Eastern", "Northern", "Western"]


@router.get("/status", summary="Engine status overview (read-only)")
def engine_status():
    items = load_items()
    locks = load_region_locks()

    regions = {}

    for region in REGIONS:
        snapshot = load_region_snapshot(region)
        regions[region] = {
            "locked": bool(locks.get(region, False)),
            "snapshot_exists": snapshot is not None,
            "snapshot_count": len(snapshot) if snapshot else 0
        }

    return {
        "status": "ok",
        "timestamp_utc": datetime.utcnow().isoformat(),
        "top100_count": len(items),
        "regions": regions
    }