from fastapi import APIRouter
from pydantic import BaseModel, field_validator
from datetime import datetime
import json
import os

router = APIRouter()

TV_FILE = "data/tv.json"
BOOST_FILE = "data/boost.json"

TV_WEIGHT = 4  # TV impact (between radio and youtube)


class TVPlay(BaseModel):
    title: str
    artist: str
    plays: int

    @field_validator("plays")
    @classmethod
    def positive(cls, v):
        if v <= 0:
            raise ValueError("plays must be positive")
        return v


def load_json(path):
    if not os.path.exists(path):
        return []
    with open(path, "r") as f:
        return json.load(f)


def save_json(path, data):
    os.makedirs("data", exist_ok=True)
    with open(path, "w") as f:
        json.dump(data, f, indent=2)


@router.post("/ingest/tv")
def ingest_tv(payload: TVPlay):
    tv = load_json(TV_FILE)
    boosts = load_json(BOOST_FILE)

    tv.append({
        "title": payload.title,
        "artist": payload.artist,
        "plays": payload.plays,
        "time": datetime.utcnow().isoformat()
    })

    boosts.append({
        "title": payload.title,
        "artist": payload.artist,
        "points": payload.plays * TV_WEIGHT,
        "source": "tv",
        "time": datetime.utcnow().isoformat()
    })

    save_json(TV_FILE, tv)
    save_json(BOOST_FILE, boosts)

    return {
        "status": "tv ingested",
        "title": payload.title,
        "artist": payload.artist,
        "points_added": payload.plays * TV_WEIGHT
    }