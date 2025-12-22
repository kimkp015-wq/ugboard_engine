from fastapi import APIRouter, HTTPException
from data.region_store import get_region

router = APIRouter()


@router.get("/regions/{region}")
def get_region_chart(region: str):
    chart = get_region(region.lower())
    if not chart:
        raise HTTPException(
            status_code=404,
            detail="Region chart not published yet"
        )
    return chart