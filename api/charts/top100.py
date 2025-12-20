import json
import os
from fastapi import APIRouter, HTTPException

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

    try:
        with open(DATA_FILE, "r") as f:
            data = json.load(f)
    except Exception:
        raise HTTPException(status_code=500, detail="Failed to read Top 100 data")

    return {
        "status": "ok",
        "count": len(data.get("items", [])),
        "items": data.get("items", [])
    }