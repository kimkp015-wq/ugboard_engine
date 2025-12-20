from fastapi import APIRouter, HTTPException
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
        raise HTTPException(
            status_code=500,
            detail="Top100 file not found in any known location"
        )

    try:
        with open(path, "r") as f:
            data = json.load(f)
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to read Top100 file: {str(e)}"
        )

    items = data.get("items", [])

    return {
        "status": "ok",
        "count": len(items),
        "items": items
    }


@router.post("/top100/recalculate")
def recalculate_top100():
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
            detail=f"Failed to read Top100 file: {str(e)}"
        )

    items = data.get("items", [])

    if not isinstance(items, list):
        raise HTTPException(
            status_code=500,
            detail="Invalid Top100 format"
        )

    # Apply boosts ONLY if available
    if apply_boosts is not None:
        try:
            items = apply_boosts(items)
        except Exception:
            pass  # boosts must NEVER crash charts

    # Sort by score
    def score_value(item):
        try:
            return float(item.get("score", 0))
        except Exception:
            return 0

    items = sorted(items, key=score_value, reverse=True)

    # Reassign positions
    for index, item in enumerate(items, start=1):
        item["position"] = index

    data["items"] = items

    try:
        with open(path, "w") as f:
            json.dump(data, f, indent=2)
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to write Top100 file: {str(e)}"
        )

    return {
        "status": "recalculated",
        "count": len(items)
    }