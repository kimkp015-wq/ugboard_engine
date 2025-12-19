from fastapi import APIRouter, HTTPException
import json
import os

router = APIRouter()

DATA_FILE = "data/top100.json"


def calculate_score(youtube: int = 0, radio: int = 0, tv: int = 0) -> float:
    """
    Simple weighted scoring system
    """
    if youtube < 0 or radio < 0 or tv < 0:
        raise ValueError("Metrics must be non-negative")

    return (youtube * 0.5) + (radio * 0.3) + (tv * 0.2)


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

    songs = data.get("items", [])
    scored_items = []

    for song in songs:
        score = calculate_score(
            youtube=song.get("youtube", 0),
            radio=song.get("radio", 0),
            tv=song.get("tv", 0),
        )

        scored_items.append({
            "position": song.get("position"),
            "title": song.get("title"),
            "artist": song.get("artist"),
            "score": score,
            "youtube": song.get("youtube", 0),
            "radio": song.get("radio", 0),
            "tv": song.get("tv", 0),
        })

    # 🔥 SORT BY SCORE (HIGHEST FIRST)
    scored_items = sorted(scored_items, key=lambda x: x["score"], reverse=True)

    # 🔥 REASSIGN POSITIONS
    for i, item in enumerate(scored_items, start=1):
        item["position"] = i

    return {
        "status": "ok",
        "count": len(scored_items),
        "items": scored_items
    }