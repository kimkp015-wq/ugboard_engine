# api/charts/regions.py

from fastapi import APIRouter
from data.store import load_items
from data.region_store import is_region_locked

router = APIRouter()

VALID_REGIONS = ["Eastern", "Western", "Northern"]


@router.get("/regions/{region}")
def get_region_chart(region: str):
    """
    Returns Top 5 songs for a given region.
    READ-ONLY.
    """

    if region not in VALID_REGIONS:
        return {
            "region": region,
            "locked": False,
            "chart": []
        }

    items = load_items()

    # Filter by region
    regional_songs = [
        song for song in items
        if song.get("region") == region
    ]

    # Sort by score DESC
    regional_songs.sort(
        key=lambda x: x.get("score", 0),
        reverse=True
    )

    top_5 = regional_songs[:5]

    return {
        "region": region,
        "locked": is_region_locked(region),
        "chart": top_5
    }