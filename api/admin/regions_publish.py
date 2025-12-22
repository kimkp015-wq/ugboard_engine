from fastapi import APIRouter, HTTPException

from data.store import load_items
from data.region_store import lock_region, is_region_locked
from data.region_snapshots import save_region_snapshot

router = APIRouter()

VALID_REGIONS = ["Eastern", "Northern", "Western"]


@router.post("/regions/{region}/publish")
def publish_region_chart(region: str):
    region = region.title()

    if region not in VALID_REGIONS:
        raise HTTPException(
            status_code=400,
            detail="Invalid region"
        )

    if is_region_locked(region):
        raise HTTPException(
            status_code=409,
            detail=f"{region} region already published"
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

    save_region_snapshot(region, top5)
    lock_region(region)

    return {
        "status": "ok",
        "region": region,
        "published": True,
        "count": len(top5)
    }