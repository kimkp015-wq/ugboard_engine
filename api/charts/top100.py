from fastapi import APIRouter
import json
import os

router = APIRouter()

TOP100_FILE = "data/top100.json"
YOUTUBE_FILE = "data/youtube.json"


@router.get("/charts/top100")
def get_top100():
    if not os.path.exists(TOP100_FILE):
        return {"status": "ok", "count": 0, "items": []}

    with open(TOP100_FILE, "r") as f:
        top100 = json.load(f)

    youtube_scores = {}

    if os.path.exists(YOUTUBE_FILE):
        with open(YOUTUBE_FILE, "r") as f:
            yt_data = json.load(f)
            for item in yt_data:
                key = f"{item['title'].lower()}::{item['artist'].lower()}"
                youtube_scores[key] = youtube_scores.get(key, 0) + item["score"]

    enriched_items = []

    for song in top100.get("items", []):
        key = f"{song['title'].lower()}::{song['artist'].lower()}"
        yt_score = youtube_scores.get(key, 0)

        enriched_items.append({
            **song,
            "youtube_score": yt_score,
            "total_score": yt_score
        })

    # sort by total score (desc)
    enriched_items.sort(key=lambda x: x["total_score"], reverse=True)

    return {
        "status": "ok",
        "count": len(enriched_items),
        "items": enriched_items
    }