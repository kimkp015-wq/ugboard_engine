items = recalculate_all(items)
from fastapi import APIRouter
from typing import List, Union
from pydantic import BaseModel

from store import load_items,items = recalculate_all(items) save_items

router = APIRouter()


# -------- DATA MODELS --------

class TVItem(BaseModel):
    title: str
    artist: str
    plays: int


class BulkTV(BaseModel):
    items: List[TVItem]


# -------- HELPERS --------

def find_song(items, title, artist):
    for item in items:
        if item["title"] == title and item["artist"] == artist:
            return item
    return None


# -------- ROUTE --------

@router.post("/ingest/tv")
def ingest_tv(payload: Union[TVItem, BulkTV]):
    items = load_items()
    ingested = 0

    # Normalize to list
    if isinstance(payload, BulkTV):
        incoming = payload.items
    else:
        incoming = [payload]

    for entry in incoming:
        song = find_song(items, entry.title, entry.artist)

        if song:
            song["tv"] = song.get("tv", 0) + entry.plays
        else:
            items.append({
                "title": entry.title,
                "artist": entry.artist,
                "youtube": 0,
                "radio": 0,
                "tv": entry.plays,
                "score": 0,
            })

        ingested += 1

    save_items(items)

    return {
        "status": "ok",
        "ingested": ingested
    }