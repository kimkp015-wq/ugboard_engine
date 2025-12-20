from fastapi import APIRouter
import json
import os

router = APIRouter()

DATA_FILE = "data/top100.json"


@router.get("/charts/top100")
def top100():
    if not os.path.exists(DATA_FILE):
        return {
            "status": "ok",
            "count": 0,
            "items": []
        }

    with open(DATA_FILE, "r") as f:
        data = json.load(f)

    return {
        "status": "ok",
        "count": len(data.get("items", [])),
        "items": data.get("items", [])
    }