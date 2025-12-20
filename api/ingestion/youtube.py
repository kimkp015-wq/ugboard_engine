from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import List, Optional
import json
import os

router = APIRouter()


# ---------- MODELS ----------

class YouTubeItem(BaseModel):
    title: str
    artist: str
    views: int


class YouTubeBulk(BaseModel):
    items: List[YouTubeItem]


# ---------- FILE RESOLUTION ----------

def resolve_top100_path():
    candidates = [
        "api/data/top100.json",
        "data/top100.json",
        "ingestion/top100.json",
        "/app/api/data/top100.json",
        "/app/data/top100.json",
        "/app/ingestion/top100.json",
    ]
    for path in candidates:
        if os.path.exists(path):
            return path
    return None


# ---------- HELPERS ----------

def load_top100(path):
    with open(path, "r") as f:
        return json.load(f)


def save_top100(path, data):
    with open(path, "w") as f:
        json.dump(data, f, indent=2)


def find_song(items, title, artist):
    for item in items:
        if (
            item.get("title") == title
            and item.get("artist") == artist
        ):
            return item
    return None


# ---------- ROUTE ----------

@router.post("/youtube")
def ingest_youtube(payload: Optional[YouTubeBulk] = None):
    """
    Accepts:
    - Single item
    - Bulk items under { "items": [...] }
    """

    path = resolve_top100_path()
    if not path:
        raise HTTPException(status_code=500, detail="Top100 file not found")

    data = load_top100(path)
    items = data.get("items", [])

    if not isinstance(items, list):
        items = []

    # Normalize payload
    if payload is None or not payload.items:
        raise HTTPException(
            status_code=422,
            detail="Body must contain { items: [...] }"
        )

    ingested = 0

    for entry in payload.items:
        song = find_song(items, entry.title, entry.artist)

        if song:
            # Update existing
            song["youtube"] = song.get("youtube", 0) + entry.views
        else:
            # Create new
            items.append({
                "title": entry.title,
                "artist": entry.artist,
                "youtube": entry.views,
                "radio": 0,
                "tv": 0,
                "score": 0
            })

        ingested += 1

    data["items"] = items
    save_top100(path, data)

    return {
        "status": "ok",
        "ingested": ingested
    }