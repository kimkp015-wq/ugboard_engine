from fastapi import APIRouter
import json
import os
from datetime import datetime

router = APIRouter()

DATA_FILE = "data/top100.json"


def current_week():
    now = datetime.utcnow()
    year, week, _ = now.isocalendar()
    return f"{year}-{week}"


def calculate_score(song):
    score = 0
    score += song.get("radio", 0) * 5
    score += song.get("tv", 0) * 5
    score += song.get("youtube", 0) * 3
    score += song.get("streams", 0) * 2
    score += song.get("boost", 0) * 10
    return score


def reset_metrics(song):
    song["radio"] = 0
    song["tv"] = 0
    song["youtube"] = 0
    song["streams"] = 0
    song["boost"] = 0
    return song


@router.get("/charts/top100")
def get_top100():
    week_now = current_week()

    if not os.path.exists(DATA_FILE):
        return {
            "status": "ok",
            "week": week_now,
            "items": []
        }

    with open(DATA_FILE, "r") as f:
        data = json.load(f)

    saved_week = data.get("week")

    # new week detected → reset
    if saved_week != week_now:
        for song in data.get("items", []):
            reset_metrics(song)

        data["week"] = week_now

        with open(DATA_FILE, "w") as f:
            json.dump(data, f, indent=2)

    songs = data.get("items", [])

    for song in songs:
        song["score"] = calculate_score(song)

    ranked = sorted(songs, key=lambda x: x["score"], reverse=True)

    for idx, song in enumerate(ranked, start=1):
        song["position"] = idx

    return {
        "status": "ok",
        "week": week_now,
        "count": len(ranked),
        "items": ranked[:100]
    }