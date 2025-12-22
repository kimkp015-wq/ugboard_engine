from fastapi import APIRouter, HTTPException
from data.store import load_items
from data.region_store import is_region_locked

router = APIRouter()

VALID_REGIONS = {"Eastern", "Northern", "Western"}

@router.get("/regions/{region}")
def get_region_chart(region: str):
    if region not in VALID_REGIONS:
        raise HTTPException(status_code=400, detail="Invalid region")

    if is_region_locked(region):
        return {
            "region": region,
            "locked": True,
            "items": []
        }

    items = load_items()

    region_items = [
        i for i in items
        if i.get("region") == region
    ]

    region_items = sorted(
        region_items,
        key=lambda x: x.get("score", 0),
        reverse=True
    )[:5]

    return {
        "region": region,
        "locked": False,
        "count": len(region_items),
        "items": region_items
    }