from fastapi import APIRouter, HTTPException
import json
import os

router = APIRouter()

TOP100_PATH = "data/top100.json"


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

    os.makedirs("data", exist_ok=True)

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


@router.post("/publish/top100/lock")
def lock_top100():
    if not os.path.exists(TOP100_PATH):
        raise HTTPException(
            status_code=404,
            detail="Top100 not published yet"
        )

    with open(TOP100_PATH, "r") as f:
        data = json.load(f)

    data["locked"] = True

    with open(TOP100_PATH, "w") as f:
        json.dump(data, f, indent=2)

    return {
        "status": "locked"
    }