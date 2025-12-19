from fastapi import APIRouter
from typing import List
import json
import os

router = APIRouter()

DATA_FILE = "data/top100.json"


def calculate_score(youtube: int, radio: int, tv: int) -> int:
    return int(
        (youtube * 0.5) +
        (radio * 0.3) +
        (tv * 0.2)
    )


@router.get("/top100")
def get_top100():
    if not os.path.exists(DATA_FILE):
        return {
            "status": "ok",
            "count": 0,
            "items": []
        }

    with open(DATA_FILE, "r") as f:
        data = json.load(f)

    items = data.get("items", [])

    scored_items = []

    for song in items:
        score = calculate_score(
            youtube=song.get("youtube", 0),
            radio=song.get("radio", 0),
            tv=song.get("tv", 0)
        )

        scored_items.append({
            "title": song["title"],
            "artist": song["artist"],
            "youtube": song.get("youtube", 0),
            "radio": song.get("radio", 0),
            "tv": song.get("tv", 0),
            "score": score
        })

    # sort by score (highest first)
    scored_items.sort(key=lambda x: x["score"], reverse=True)

    # assign chart positions
    for i, item in enumerate(scored_items, start=1):
        item["position"] = i

    return {
        "status": "ok",
        "count": len(scored_items),
        "items": scored_items[:100]
    }