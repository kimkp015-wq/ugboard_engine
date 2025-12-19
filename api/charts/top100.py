from fastapi import APIRouter
import json
import os

router = APIRouter()

TOP100_FILE = "data/top100.json"
YOUTUBE_FILE = "data/youtube.json"


@router.get("/charts/top100")
def charts_top100():
    if not os.path.exists(TOP100_FILE):
        return {"status": "ok", "count": 0, "items": []}

    with open(TOP100_FILE, "r") as f:
        top100 = json.load(f)

    youtube_scores = {}

    if os.path.exists(YOUTUBE_FILE):
        with open(YOUTUBE_FILE, "r") as f:
            yt_data = json.load(f)
            for row in yt_data:
                key = f"{row['title'].lower()}::{row['artist'].lower()}"
                youtube_scores[key] = youtube_scores.get(key, 0) + row.get("score", 0)

    enriched = []

    for song in top100.get("items", []):
        key = f"{song['title'].lower()}::{song['artist'].lower()}"
        yt = youtube_scores.get(key, 0)

        enriched.append({
            "position": song["position"],
            "title": song["title"],
            "artist": song["artist"],
            "youtube": yt,
            "radio": 0,
            "tv": 0,
            "score": yt
        })

    enriched.sort(key=lambda x: x["score"], reverse=True)

    return {
        "status": "ok",
        "count": len(enriched),
        "items": enriched
    }