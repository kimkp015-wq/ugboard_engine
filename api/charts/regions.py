# api/charts/regions.py

from fastapi import APIRouter, HTTPException
from data.store import load_items
from data.region_store import is_region_locked
from data.region_snapshots import load_region_snapshot

router = APIRouter()

VALID_REGIONS = ["Eastern", "Northern", "Western"]


@router.get("/regions/{region}", summary="Get Top 5 songs per region")
def get_region_chart(region: str):
    region = region.title()

    if region not in VALID_REGIONS:
        raise HTTPException(
            status_code=400,
            detail="Invalid region name"
        )

    # If region is locked, serve snapshot
    if is_region_locked(region):
        snapshot = load_region_snapshot(region)
        if snapshot is None:
            raise HTTPException(
                status_code=404,
                detail="Region snapshot not found"
            )

        return {
            "status": "ok",
            "region": region,
            "locked": True,
            "count": len(snapshot),
            "items": snapshot
        }

    # Otherwise return live Top 5
    items = load_items()

    region_items = [
        i for i in items
        if i.get("region") == region
    ]

    region_items.sort(
        key=lambda x: x.get("score", 0),
        reverse=True
    )

    top5 = region_items[:5]

    return {
        "status": "ok",
        "region": region,
        "locked": False,
        "count": len(top5),
        "items": top5
    }