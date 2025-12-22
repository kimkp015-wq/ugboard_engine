from fastapi import APIRouter, HTTPException

from data.store import load_items
from data.region_store import is_region_locked, lock_region
from data.region_snapshots import save_region_snapshot
from data.region_publish_state import (
    was_region_published_this_week,
    mark_region_published
)

router = APIRouter()

VALID_REGIONS = ["Eastern", "Northern", "Western"]


@router.post("/regions/{region}/publish", summary="Publish & freeze regional Top 5 (weekly)")
def publish_region(region: str):
    region = region.title()

    if region not in VALID_REGIONS:
        raise HTTPException(
            status_code=400,
            detail="Invalid region"
        )

    # 🔒 HARD WEEKLY GUARD (EAT)
    if was_region_published_this_week(region):
        raise HTTPException(
            status_code=409,
            detail=f"{region} already published this week"
        )

    # 🔒 Already locked guard
    if is_region_locked(region):
        raise HTTPException(
            status_code=409,
            detail=f"{region} region already locked"
        )

    items = load_items()

    region_songs = [
        i for i in items
        if i.get("region") == region
    ]

    if not region_songs:
        raise HTTPException(
            status_code=400,
            detail="No songs available for region"
        )

    top5 = sorted(
        region_songs,
        key=lambda x: x.get("score", 0),
        reverse=True
    )[:5]

    # 📸 Freeze snapshot
    save_region_snapshot(region, top5)

    # 🔒 Lock region
    lock_region(region)

    # 🗓️ Mark weekly publish (EAT)
    mark_region_published(region)

    return {
        "status": "ok",
        "region": region,
        "published": True,
        "locked": True,
        "count": len(top5)
    }