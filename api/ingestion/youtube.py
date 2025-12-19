from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, field_validator
from datetime import datetime
import json
import os

router = APIRouter()

DATA_FILE = "data/youtube.json"


class YouTubeIngest(BaseModel):
    title: str
    artist: str
    views: int

    @field_validator("title", "artist")
    @classmethod
    def not_empty(cls, v):
        if not v or not v.strip():
            raise ValueError("title and artist cannot be empty")
        return v.strip()

    @field_validator("views")
    @classmethod
    def views_positive(cls, v):
        if v < 0:
            raise ValueError("views must be positive")
        return v


@router.post("/ingest/youtube")
def ingest_youtube(payload: YouTubeIngest):
    os.makedirs("data", exist_ok=True)

    # simple scoring rule (safe & free)
    score = payload.views // 1000  # 1 point per 1,000 views

    entry = {
        "title": payload.title,
        "artist": payload.artist,
        "views": payload.views,
        "score": score,
        "ingested_at": datetime.utcnow().isoformat()
    }

    data = []
    if os.path.exists(DATA_FILE):
        with open(DATA_FILE, "r") as f:
            data = json.load(f)

    data.append(entry)

    with open(DATA_FILE, "w") as f:
        json.dump(data, f, indent=2)

    return {
        "status": "ok",
        "score_added": score
    }