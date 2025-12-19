from fastapi import APIRouter, HTTPException
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
    return (
        song.get("radio", 0) * 5 +
        song.get("tv", 0) * 5 +
        song.get("youtube", 0) * 3 +
        song.get("streams", 0) * 2 +
        song.get("boost", 0) * 10
    )


def reset_metrics(song):
    for k in ["radio", "tv", "youtube", "streams", "boost"]:
        song[k] = 0
    return song


@router.get("/charts/top100")
def get_top100():
    week_now = current_week()

    if not os.path.exists(DATA_FILE):
        return {"status": "ok", "week": week_now, "items": []}

    with open(DATA_FILE, "r") as f:
        data = json.load(f)

    # New week → unlock + reset
    if data.get("week") != week_now:
        data["week"] = week_now
        data["locked"] = False
        for song in data.get("items", []):
            reset_metrics(song)

        with open(DATA_FILE, "w") as f:
            json.dump(data, f, indent=2)

    songs = data.get("items", [])

    for song in songs:
        song["score"] = calculate_score(song)

    ranked = sorted(songs, key=lambda x: x["score"], reverse=True)

    for i, song in enumerate(ranked, start=1):
        song["position"] = i

    return {
        "status": "ok",
        "week": data.get("week"),
        "locked": data.get("locked", False),
        "items": ranked[:100]
    }