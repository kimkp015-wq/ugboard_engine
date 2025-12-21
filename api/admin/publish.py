from fastapi import APIRouter, HTTPException
import json
from pathlib import Path

router = APIRouter()

TOP100_PATH = Path("data/top100.json")


# -----------------------------
# Publish Top100
# -----------------------------
@router.post("/publish/top100")
def publish_top100(payload: dict):
    items = payload.get("items")

    if not isinstance(items, list) or len(items) == 0:
        raise HTTPException(
            status_code=400,
            detail="items must be a non-empty list"
        )

    clean_items = []

    for index, item in enumerate(items, start=1):
        title = item.get("title")
        artist = item.get("artist")

        if not title or not artist:
            continue

        clean_items.append({
            "position": index,
            "title": title,
            "artist": artist,
            "youtube": 0,
            "radio": 0,
            "tv": 0,
            "score": 0
        })

    if not clean_items:
        raise HTTPException(
            status_code=400,
            detail="No valid items to publish"
        )

    TOP100_PATH.parent.mkdir(parents=True, exist_ok=True)

    with open(TOP100_PATH, "w") as f:
        json.dump(
            {
                "locked": False,
                "items": clean_items
            },
            f,
            indent=2
        )

    return {
        "status": "ok",
        "published": len(clean_items)
    }


# -----------------------------
# Lock Top100
# -----------------------------
@router.post("/publish/top100/lock")
def lock_top100():
    if not TOP100_PATH.exists():
        raise HTTPException(
            status_code=404,
            detail="Top100 not published yet"
        )

    with open(TOP100_PATH, "r") as f:
        data = json.load(f)

    if data.get("locked") is True:
        return {
            "status": "already_locked"
        }

    data["locked"] = True

    with open(TOP100_PATH, "w") as f:
        json.dump(data, f, indent=2)

    return {
        "status": "locked"
    }