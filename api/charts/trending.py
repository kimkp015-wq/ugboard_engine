from fastapi import APIRouter
import json
import os

router = APIRouter()

TRENDING_PATH = "data/trending.json"


@router.get("/trending")
def get_trending():
    if not os.path.exists(TRENDING_PATH):
        return {
            "status": "ok",
            "count": 0,
            "items": []
        }

    with open(TRENDING_PATH, "r") as f:
        data = json.load(f)

    items = data.get("items", [])

    return {
        "status": "ok",
        "count": len(items),
        "items": items
    }