from api.scoring.scoring import recalculate_all
from fastapi import APIRouter, HTTPException
from typing import List, Union
from pydantic import BaseModel

from store import load_items,items = recalculate_all(items) save_items

router = APIRouter()


# -------- DATA MODELS --------

class YouTubeItem(BaseModel):
    title: str
    artist: str
    views: int


class BulkYouTube(BaseModel):
    items: List[YouTubeItem]


# -------- HELPERS --------

def find_song(items, title, artist):
    for item in items:
        if item["title"] == title and item["artist"] == artist:
            return item
    return None


# -------- ROUTE --------

@router.post("/ingest/youtube")
def ingest_youtube(payload: Union[YouTubeItem, BulkYouTube]):
    items = load_items()
    ingested = 0

    # Normalize to list
    if isinstance(payload, BulkYouTube):
        incoming = payload.items
    else:
        incoming = [payload]

    for entry in incoming:
        song = find_song(items, entry.title, entry.artist)

        if song:
            song["youtube"] = song.get("youtube", 0) + entry.views
        else:
            items.append({
                "title": entry.title,
                "artist": entry.artist,
                "youtube": entry.views,
                "radio": 0,
                "tv": 0,
                "score": 0,
            })

        ingested += 1

    save_items(items)

    return {
        "status": "ok",
        "ingested": ingested
    }