from fastapi import APIRouter, HTTPException
import json
import os

router = APIRouter()

# Absolute path to api/data/top100.json
BASE_DIR = os.path.dirname(os.path.dirname(__file__))  # api/
DATA_FILE = os.path.join(BASE_DIR, "data", "top100.json")


@router.get("/top100")
def get_top100():
    if not os.path.exists(DATA_FILE):
        raise HTTPException(
            status_code=500,
            detail=f"Top100 file not found at {DATA_FILE}"
        )

    try:
        with open(DATA_FILE, "r") as f:
            data = json.load(f)
    except Exception:
        raise HTTPException(
            status_code=500,
            detail="Failed to read Top 100 data"
        )

    items = data.get("items", [])

    return {
        "status": "ok",
        "count": len(items),
        "items": items
    }