from fastapi import APIRouter, HTTPException
from data.store import load_items
from data.region_store import lock_region, is_region_locked
from data.region_snapshots import save_region_snapshot

router = APIRouter()

@router.post("/regions/{region}/publish")
def publish_region(region: str):
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

    top5 = sorted(
        region_songs,
        key=lambda x: x.get("score", 0),
        reverse=True
    )[:5]

    if not top5:
        raise HTTPException(
            status_code=400,
            detail="No songs available for region"
        )

    save_region_snapshot(region, top5)
    lock_region(region)

    return {
        "status": "ok",
        "region": region,
        "count": len(top5)
    }