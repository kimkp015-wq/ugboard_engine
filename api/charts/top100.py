from fastapi import APIRouter, HTTPException
from api.scoring.scoring import calculate_score
import json
import os

router = APIRouter()

# --- SAFE OPTIONAL BOOST IMPORT ---
try:
    from api.charts.boost import apply_boosts
except Exception:
    apply_boosts = None


def resolve_top100_path():
    candidates = [
        "api/data/top100.json",
        "data/top100.json",
        "ingestion/top100.json",
        "/app/api/data/top100.json",
        "/app/data/top100.json",
        "/app/ingestion/top100.json",
    ]
    for path in candidates:
        if os.path.exists(path):
            return path
    return None


@router.get("/top100")
def get_top100():
    path = resolve_top100_path()
    if not path:
        raise HTTPException(500, "Top100 file not found")

    with open(path, "r") as f:
        data = json.load(f)

    items = data.get("items", [])
    return {"status": "ok", "count": len(items), "items": items}


@router.post("/top100/recalculate")
def recalculate_top100():
    path = resolve_top100_path()
    if not path:
        raise HTTPException(500, "Top100 file not found")

    with open(path, "r") as f:
        data = json.load(f)

    items = data.get("items", [])
    if not isinstance(items, list):
        raise HTTPException(500, "Invalid Top100 format")

    for item in items:
        try:
            item["score"] = calculate_score(item)
        except Exception:
            item["score"] = 0

    if apply_boosts:
        try:
            items = apply_boosts(items)
        except Exception:
            pass

    items = sorted(items, key=lambda x: float(x.get("score", 0)), reverse=True)

    for i, item in enumerate(items, start=1):
        item["position"] = i

    data["items"] = items

    with open(path, "w") as f:
        json.dump(data, f, indent=2)

    return {"status": "recalculated", "count": len(items)}