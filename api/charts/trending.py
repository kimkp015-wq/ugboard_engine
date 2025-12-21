from fastapi import APIRouter
import json
import os

router = APIRouter()

TOP100_PATH = "data/top100.json"


@router.get("/trending")
def get_trending():
    if not os.path.exists(TOP100_PATH):
        return {
            "status": "ok",
            "count": 0,
            "items": []
        }

    with open(TOP100_PATH, "r") as f:
        data = json.load(f)

    items = data.get("items", [])

    # Trending = top 10 by position/score (already sorted)
    return {
        "status": "ok",
        "count": min(10, len(items)),
        "items": items[:10]
    }