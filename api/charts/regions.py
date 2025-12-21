from fastapi import APIRouter
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


@router.get("/regions")
def get_regions():
    path = resolve_top100_path()

    if not path:
        return {"chart": "regions", "items": []}

    try:
        with open(path, "r") as f:
            data = json.load(f)
    except Exception:
        return {"chart": "regions", "items": []}

    return {
        "chart": "regions",
        "items": data.get("items", [])
    }