# api/charts/regions.py

from fastapi import APIRouter, HTTPException
from data.region_store import get_region

router = APIRouter()

VALID_REGIONS = ["Eastern", "Northern", "Western"]


@router.get("/regions/{region}")
def get_region_chart(region: str):
    if region not in VALID_REGIONS:
        raise HTTPException(status_code=400, detail="Invalid region")

    data = get_region(region)

    if not data:
        return {
            "status": "pending",
            "region": region,
            "items": []
        }

    return {
        "status": "ok",
        "region": region,
        "week": data["week"],
        "items": data["items"]
    }