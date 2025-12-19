from fastapi import APIRouter
import json
import os

router = APIRouter()

DATA_FILE = "data/top100.json"


def calculate_score(song):
    score = 0

    score += song.get("radio", 0) * 5
    score += song.get("tv", 0) * 5
    score += song.get("youtube", 0) * 3
    score += song.get("streams", 0) * 2
    score += song.get("boost", 0) * 10

    return score


@router.get("/charts/top100")
def get_top100():
    if not os.path.exists(DATA_FILE):
        return {
            "status": "ok",
            "items": []
        }

    with open(DATA_FILE, "r") as f:
        data = json.load(f)

    songs = data.get("items", [])

    # calculate score
    for song in songs:
        song["score"] = calculate_score(song)

    # sort by score (highest first)
    ranked = sorted(songs, key=lambda x: x["score"], reverse=True)

    # assign positions
    for idx, song in enumerate(ranked, start=1):
        song["position"] = idx

    return {
        "status": "ok",
        "count": len(ranked),
        "items": ranked[:100]
    }