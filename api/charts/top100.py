from fastapi import APIRouter, HTTPException
import json
import os
from api.scoring.scoring import calculate_score

router = APIRouter()


def resolve_top100_path():
    """
    Try all known valid locations.
    This avoids Railway path issues.
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
    # ✅ FIXED FUNCTION NAME
    path = resolve_top100_path()

    if not path:
        raise HTTPException(
            status_code=500,
            detail="Top100 file not found in any known location"
        )

    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to read Top100 file: {str(e)}"
        )

    items = data.get("items", [])

for item in items:
    item["score"] = calculate_score(
        youtube=item.get("youtube", 0),
        radio=item.get("radio", 0),
        tv=item.get("tv", 0),
    )
    # sort by score (highest first)
items.sort(key=lambda x: x["score"], reverse=True)

# reassign chart positions
for index, item in enumerate(items, start=1):
    item["position"] = index

    return {
        "status": "ok",
        "count": len(items),
        "items": items
    }