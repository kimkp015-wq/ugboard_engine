from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, field_validator
from datetime import datetime
import json
import os

router = APIRouter()

BOOST_FILE = "data/boost.json"
TOP100_FILE = "data/top100.json"


class BoostRequest(BaseModel):
    title: str
    artist: str
    points: int

    @field_validator("points")
    @classmethod
    def positive_points(cls, v):
        if v <= 0:
            raise ValueError("boost points must be positive")
        return v


def load_boosts():
    if not os.path.exists(BOOST_FILE):
        return []
    with open(BOOST_FILE, "r") as f:
        return json.load(f)


def save_boosts(data):
    os.makedirs("data", exist_ok=True)
    with open(BOOST_FILE, "w") as f:
        json.dump(data, f, indent=2)


def rebuild_top100(boosts):
    # group by song
    scores = {}

    for b in boosts:
key = b["title"] + "|" + b["artist"]
            "title": b["title"],
            "artist": b["artist"],
            "score": 0
        })
        scores[key]["score"] += b["points"]

    # sort by score
    ranked = sorted(scores.values(), key=lambda x: x["score"], reverse=True)

    items = []
    for idx, song in enumerate(ranked[:100], start=1):
        items.append({
            "position": idx,
            "title": song["title"],
            "artist": song["artist"],
            "score": song["score"]
        })

    data = {
        "updated_at": datetime.utcnow().isoformat(),
        "count": len(items),
        "items": items
    }

    with open(TOP100_FILE, "w") as f:
        json.dump(data, f, indent=2)


@router.post("/boost")
def boost_song(payload: BoostRequest):
    boosts = load_boosts()

    boosts.append({
        "title": payload.title,
        "artist": payload.artist,
        "points": payload.points,
        "time": datetime.utcnow().isoformat()
    })

    save_boosts(boosts)

    # 🔥 THIS IS THE IMPORTANT PART
    rebuild_top100(boosts)

    return {
        "status": "boosted",
        "title": payload.title,
        "artist": payload.artist,
        "points": payload.points
    }