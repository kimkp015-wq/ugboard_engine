from fastapi import APIRouter, HTTPException
from data.region_store import publish_region, is_frozen
from pathlib import Path
import json
from datetime import date

router = APIRouter()

ITEMS_FILE = Path("data/items.json")


@router.post("/publish/region/{region}")
def publish_region_chart(region: str):
    week = date.today().isoformat()

    if is_frozen(region, week):
        raise HTTPException(
            status_code=409,
            detail="Region chart already published for this week"
        )

    if not ITEMS_FILE.exists():
        raise HTTPException(status_code=500, detail="Items store missing")

    items = json.loads(ITEMS_FILE.read_text())

    # Filter by region
    region_items = [
        v for v in items.values()
        if v.get("region") == region.lower()
    ]

    # Sort by score DESC
    region_items.sort(key=lambda x: x.get("score", 0), reverse=True)

    top5 = region_items[:5]

    publish_region(region.lower(), top5, week)

    return {
        "status": "published",
        "region": region.lower(),
        "count": len(top5)
    }