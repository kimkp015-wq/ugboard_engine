# api/charts/regions.py

from fastapi import APIRouter
from data.region_snapshots import load_region_snapshots

router = APIRouter()


@router.get("/regions")
def get_region_charts():
    """
    Read-only region charts.
    Data is served from region snapshots.
    """
    charts = load_region_snapshots()

    return {
        "status": "ok",
        "regions": {
            "Eastern": charts.get("Eastern", [])[:5],
            "Northern": charts.get("Northern", [])[:5],
            "Western": charts.get("Western", [])[:5],
        }
    }