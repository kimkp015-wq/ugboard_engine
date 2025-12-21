from fastapi import APIRouter
import json
import os
from data.store import load_items, save_items

router = APIRouter()

DATA_FILE = "data/trending.json"


@router.get("/trending")
def get_trending():
    # If file does not exist, return empty safely
    if not os.path.exists(DATA_FILE):
        return {
            "status": "ok",
            "count": 0,
            "items": []
        }

    try:
        with open(DATA_FILE, "r") as f:
            data = json.load(f)

        items = data.get("items", [])
    items = load_items()
save_items(items)
        return {
            "status": "ok",
            "count": len(items),
            "items": items
        }

    except Exception as e:
        # Never crash the API
        return {
            "status": "error",
            "message": str(e)
        }