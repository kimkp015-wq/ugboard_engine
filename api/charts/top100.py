from fastapi import APIRouter, HTTPException
import json
import os

router = APIRouter()

DATA_FILE = "data/top100.json"


@router.get("/top100")
def get_top100():
    if not os.path.exists(DATA_FILE):
        raise HTTPException(status_code=500, detail="Top100 data file missing")

    with open(DATA_FILE, "r") as f:
        data = json.load(f)

    return {
        "status": "ok",
        "count": len(data.get("items", [])),
        "updated_at": data.get("updated_at"),
        "items": data.get("items", [])
    }