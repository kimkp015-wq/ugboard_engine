from fastapi import APIRouter, HTTPException
import json
import os

# OPTIONAL boost import (safe)
try:
    from api.charts.boost import apply_boosts
except Exception:
    apply_boosts = None

router = APIRouter()


def resolve_top100_path():
    """
    Try all known valid locations.
    Avoids Railway + local path issues.
    """
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

    # --- Read JSON safely ---
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

    # --- Apply boosts if available ---
    if apply_boosts:
        try:
            items = apply_boosts(items)
        except Exception:
            # Boost failure should NOT crash charts
            pass

    return {
        "status": "ok",
        "count": len(items),
        "items": items
    }