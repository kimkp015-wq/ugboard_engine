from fastapi import APIRouter
import json
import os

router = APIRouter()

DATA_FILE = "data/top100.json"


@router.get("/charts/top100")
def get_top100():
    if not os.path.exists(DATA_FILE):
        return {
            "status": "ok",
            "count": 0,
            "items": []
        }

    with open(DATA_FILE, "r") as f:
        data = json.load(f)

    items = data.get("items", [])

    return {
        "status": "ok",
        "count": len(items),
        "items": items
    }