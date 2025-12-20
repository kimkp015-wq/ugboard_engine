from fastapi import APIRouter
import json
import os

router = APIRouter()

DATA_FILE = "data/top100.json"


# ---------- helpers ----------

def load_top100():
    if not os.path.exists(DATA_FILE):
        return []

    try:
        with open(DATA_FILE, "r") as f:
            data = json.load(f)
            return data.get("items", [])
    except Exception:
        return []


def calculate_score(item: dict) -> int:
    youtube = int(item.get("youtube", 0))
    radio = int(item.get("radio", 0))
    tv = int(item.get("tv", 0))
    boost = int(item.get("boost", 0))

    # simple + safe scoring (can improve later)
    return youtube + radio + tv + boost


# ---------- endpoint ----------

@router.get("/charts/top100")
def get_top100():
    items = load_top100()

    for item in items:
        item["score"] = calculate_score(item)

    # sort by score (highest first)
    items = sorted(items, key=lambda x: x["score"], reverse=True)

    # reassign positions
    for index, item in enumerate(items, start=1):
        item["position"] = index

    return {
        "status": "ok",
        "count": len(items),
        "items": items[:100]
    }