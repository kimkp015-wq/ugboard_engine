from fastapi import APIRouter, HTTPException
from api.charts.boost import apply_boosts
import json
import os

router = APIRouter()


def resolve_top100_path():
    """
    Try all known valid locations.
    Works locally and on Railway.
    """
    candidates = [
        "api/data/top100.json",
        "data/top100.json",
        "ingestion/top100.json",
        "/app/api/data/top100.json",
        "/app/data/top100.json",
        "/app/ingestion/top100.json",
    ]

    for path in candidates:
        if os.path.exists(path):
            return path

    return None


@router.get("/top100")
def get_top100():
    path = resolve_top100_path()

    if not path:
        raise HTTPException(
            status_code=500,
            detail="Top100 file not found in any known location"
        )

    try:
        with open(path, "r") as f:
            data = json.load(f)
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to read Top100 file: {str(e)}"
        )

    items = data.get("items", [])

    # ✅ APPLY BOOSTS HERE (SAFE LOCATION)
    items = apply_boosts(items)

    return {
        "status": "ok",
        "count": len(items),
        "items": items
    }