from fastapi import APIRouter, HTTPException
import json
import os

from api.scoring.score import calculate_score

router = APIRouter()


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
        raise HTTPException(
            status_code=500,
            detail="Top100 file not found"
        )

    try:
        with open(path, "r") as f:
            data = json.load(f)
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to read Top100 data: {str(e)}"
        )

    items = data.get("items", [])

    # 🔹 APPLY SCORING (read-time only)
    scored_items = []
    for item in items:
        item = dict(item)  # avoid mutating original
        item["score"] = calculate_score(item)
        scored_items.append(item)

    return {
        "status": "ok",
        "count": len(scored_items),
        "items": scored_items
    }