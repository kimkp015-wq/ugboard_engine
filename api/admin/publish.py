from fastapi import APIRouter, HTTPException
import json
import os
from pathlib import Path

router = APIRouter()

# Canonical Top100 storage location
TOP100_PATH = Path("data/top100.json")


@router.post("/publish/top100")
def publish_top100(payload: dict):
    """
    Publish Top100 chart manually.
    Expects:
    {
      "items": [
        { "title": "...", "artist": "..." }
      ]
    }
    """

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
            detail="No valid items found to publish"
        )

    # Ensure data directory exists
    TOP100_PATH.parent.mkdir(parents=True, exist_ok=True)

    # Write Top100 file
    with open(TOP100_PATH, "w") as f:
        json.dump(
            {
                "items": clean_items
            },
            f,
            indent=2
        )

    return {
        "status": "ok",
        "published": len(clean_items)
    }