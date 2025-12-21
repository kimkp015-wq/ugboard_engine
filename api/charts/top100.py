from fastapi import APIRouter, HTTPException
import json
import os

router = APIRouter()

TOP100_PATH = "data/top100.json"


@router.get("/top100")
def get_top100():
    if not os.path.exists(TOP100_PATH):
        return {
            "status": "ok",
            "count": 0,
            "items": []
        }

    with open(TOP100_PATH, "r") as f:
        data = json.load(f)

    items = data.get("items", [])

    return {
        "status": "ok",
        "count": len(items),
        "items": items
    }