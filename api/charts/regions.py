# api/charts/regions.py

from fastapi import APIRouter, HTTPException

from data.region_store import is_region_locked
from data.region_snapshots import load_region_snapshot

router = APIRouter()

# Canonical regions (normalized)
VALID_REGIONS = ("Eastern", "Northern", "Western")


# -------------------------
# Internal helpers
# -------------------------

def _load_items_safe():
    """
    Lazy import to avoid startup crashes.
    Live data only.
    """
    try:
        from data.store import load_items
        return load_items()
    except Exception:
        return []


def _get_week_id_safe() -> str:
    """
    Defensive week id fetch.
    Never crashes charts.
    """
    try:
        from data.chart_week import get_current_week_id
        return get_current_week_id()
    except Exception:
        return "unknown-week"


# -------------------------
# Public API
# -------------------------

@router.get(
    "/regions/{region}",
    summary="Get Top 5 songs per region",
    tags=["Charts"],
)
def get_region_chart(region: str):
    region = region.title()

    if region not in VALID_REGIONS:
        raise HTTPException(
            status_code=400,
            detail="Invalid region name",
        )

    week_id = _get_week_id_safe()

    # 🔒 Locked → serve immutable snapshot
    if is_region_locked(region):
        snapshot = load_region_snapshot(region)

        # Snapshot not yet created (safe fallback)
        if snapshot is None:
            return {
                "status": "ok",
                "week_id": week_id,
                "region": region,
                "locked": True,
                "snapshot_ready": False,
                "count": 0,
                "items": [],
            }

        return {
            "status": "ok",
            "week_id": snapshot.get("week_id", week_id),
            "region": region,
            "locked": True,
            "snapshot_ready": True,
            "count": snapshot.get("count", 0),
            "items": snapshot.get("items", []),
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
        "week_id": week_id,
        "region": region,
        "locked": False,
        "count": len(top5),
        "items": top5,
    }