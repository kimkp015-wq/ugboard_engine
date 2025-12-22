# api/admin/regions_publish.py

from fastapi import APIRouter, HTTPException
from data.store import load_items
from data.region_store import publish_region, is_region_locked

router = APIRouter()

VALID_REGIONS = ["Eastern", "Northern", "Western"]


@router.post("/admin/regions/publish/{region}")
def publish_region_chart(region: str):
    if region not in VALID_REGIONS:
        raise HTTPException(status_code=400, detail="Invalid region")

    if is_region_locked(region):
        raise HTTPException(
            status_code=409,
            detail="Region chart already published for this week"
        )

    items = load_items()

    # pull from Top 100 only
    region_items = [
        i for i in items
        if i.get("region") == region
    ]

    region_items = sorted(
        region_items,
        key=lambda x: x.get("score", 0),
        reverse=True
    )[:5]

    if not region_items:
        raise HTTPException(
            status_code=400,
            detail="No region-tagged songs available"
        )

    publish_region(region, region_items)

    return {
        "status": "ok",
        "region": region,
        "published": len(region_items)
    }