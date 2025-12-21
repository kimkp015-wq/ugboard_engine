from fastapi import APIRouter, HTTPException
from api.scoring.scoring import calculate_score
import json
import os

router = APIRouter()


# -----------------------------
# Resolve Top100 JSON path
# -----------------------------
def resolve_top100_path():
    candidates = [
        "data/top100.json",
        "/app/data/top100.json"
    ]

    for path in candidates:
        if os.path.exists(path):
            return path

    return None


# -----------------------------
# GET Top100 (SAFE)
# -----------------------------
@router.get("/top100")
def get_top100():
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
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to read Top100: {str(e)}"
        )

    items = data.get("items", [])

    return {
        "status": "ok",
        "count": len(items),
        "locked": data.get("locked", False),
        "items": items
    }


# -----------------------------
# POST Recalculate Top100
# -----------------------------
@router.post("/top100/recalculate")
def recalculate_top100():
    path = resolve_top100_path()

    if not path:
        raise HTTPException(
            status_code=404,
            detail="Top100 not published yet"
        )

    with open(path, "r") as f:
        data = json.load(f)

    # 🔒 LOCK ENFORCEMENT
    if data.get("locked") is True:
        raise HTTPException(
            status_code=403,
            detail="Top100 is locked and cannot be recalculated"
        )

    items = data.get("items", [])

    if not isinstance(items, list):
        raise HTTPException(
            status_code=500,
            detail="Invalid Top100 format"
        )

    # -----------------------------
    # Calculate scores safely
    # -----------------------------
    for item in items:
        try:
            item["score"] = calculate_score(item)
        except Exception:
            item["score"] = 0

    # -----------------------------
    # Sort by score
    # -----------------------------
    items = sorted(
        items,
        key=lambda x: float(x.get("score", 0)),
        reverse=True
    )

    # -----------------------------
    # Reassign positions
    # -----------------------------
    for index, item in enumerate(items, start=1):
        item["position"] = index

    data["items"] = items

    # -----------------------------
    # Write back
    # -----------------------------
    with open(path, "w") as f:
        json.dump(data, f, indent=2)

    return {
        "status": "recalculated",
        "count": len(items)
    }