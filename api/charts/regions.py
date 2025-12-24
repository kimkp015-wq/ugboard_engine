# api/charts/regions.py

from fastapi import APIRouter, HTTPException

from data.region_store import is_region_locked
from data.region_snapshots import load_region_snapshot

# IMPORTANT: lazy import to avoid startup crash
def _load_items_safe():
    try:
        from data.store import load_items
        return load_items()
    except Exception:
        return []

router = APIRouter()

VALID_REGIONS = ["Eastern", "Northern", "Western"]


@router.get(
    "/regions/{region}",
    summary="Get Top 5 songs per region",
)
def get_region_chart(region: str):
    region = region.title()

    if region not in VALID_REGIONS:
        raise HTTPException(
            status_code=400,
            detail="Invalid region name",
        )

    # 🔒 If region is locked, serve snapshot (if exists)
    if is_region_locked(region):
        snapshot = load_region_snapshot(region)

        # ✅ Graceful fallback if snapshot not created yet
        if snapshot is None:
            return {
                "status": "ok",
                "region": region,
                "locked": True,
                "snapshot_ready": False,
                "count": 0,
                "items": [],
            }

        return {
            "status": "ok",
            "region": region,
            "locked": True,
            "snapshot_ready": True,
            "count": len(snapshot),
            "items": snapshot,
        }

    # 🔓 Live (unlocked) region chart
    items = _load_items_safe()

    region_items = [
        i for i in items
        if i.get("region") == region
    ]

    region_items.sort(
        key=lambda x: x.get("score", 0),
        reverse=True,
    )

    top5 = region_items[:5]

    return {
        "status": "ok",
        "region": region,
        "locked": False,
        "count": len(top5),
        "items": top5,
    }