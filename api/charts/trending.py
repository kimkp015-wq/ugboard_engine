from fastapi import APIRouter, HTTPException
import json
import os

router = APIRouter()

def resolve_top100_path():
    candidates = [
        "api/data/top100.json",
        "data/top100.json",
        "/app/api/data/top100.json",
        "/app/data/top100.json",
    ]

    for path in candidates:
        if os.path.exists(path):
            return path

    return None


@router.get("/trending")
def get_trending():
    path = resolve_top100_path()

    if not path:
        return {
            "status": "ok",
            "count": 0,
            "items": []
        }

    try:
        with open(path, "r") as f:
            data = json.load(f)
    except Exception:
        return {
            "status": "ok",
            "count": 0,
            "items": []
        }

    items = data.get("items", [])

    items = sorted(
        items,
        key=lambda x: float(x.get("score", 0)),
        reverse=True
    )

    return {
        "status": "ok",
        "count": min(10, len(items)),
        "items": items[:10]
    }