from fastapi import APIRouter, HTTPException
import json
import os

# Optional boost import (safe)
try:
    from api.charts.boost import apply_boosts
except Exception:
    apply_boosts = None

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
            detail="Top100 file not found in any known location"
        )

    try:
        with open(path, "r") as f:
            data = json.load(f)
    except json.JSONDecodeError as e:
        raise HTTPException(
            status_code=500,
            detail=f"Top100 JSON is invalid: {str(e)}"
        )
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to read Top100 file: {str(e)}"
        )

    items = data.get("items", [])

    if not isinstance(items, list):
        raise HTTPException(
            status_code=500,
            detail="Top100 items must be a list"
        )

    # Apply boosts safely (optional)
    if apply_boosts:
        try:
            items = apply_boosts(items)
        except Exception:
            pass

    # -------- STEP 2C: SORT BY SCORE --------
    def score_value(item):
        try:
            return float(item.get("score", 0))
        except Exception:
            return 0

    items = sorted(items, key=score_value, reverse=True)

    # Reassign positions after sorting
    for index, item in enumerate(items, start=1):
        item["position"] = index

    return {
        "status": "ok",
        "count": len(items),
        "items": items
    }