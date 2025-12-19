from fastapi import APIRouter
from pydantic import BaseModel, field_validator
from datetime import datetime
import json
import os

router = APIRouter()

YOUTUBE_FILE = "data/youtube.json"
BOOST_FILE = "data/boost.json"

YOUTUBE_WEIGHT = 3


class YouTubePlay(BaseModel):
    title: str
    artist: str
    views: int

    @field_validator("views")
    @classmethod
    def positive(cls, v):
        if v <= 0:
            raise ValueError("views must be positive")
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


@router.post("/ingest/youtube")
def ingest_youtube(payload: YouTubePlay):
    yt = load_json(YOUTUBE_FILE)
    boosts = load_json(BOOST_FILE)

    yt.append({
        "title": payload.title,
        "artist": payload.artist,
        "views": payload.views,
        "time": datetime.utcnow().isoformat()
    })

    boosts.append({
        "title": payload.title,
        "artist": payload.artist,
        "points": payload.views * YOUTUBE_WEIGHT,
        "source": "youtube",
        "time": datetime.utcnow().isoformat()
    })

    save_json(YOUTUBE_FILE, yt)
    save_json(BOOST_FILE, boosts)

    return {
        "status": "youtube ingested",
        "title": payload.title,
        "artist": payload.artist,
        "points_added": payload.views * YOUTUBE_WEIGHT
    }