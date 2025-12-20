from fastapi import APIRouter
from typing import List, Union
from pydantic import BaseModel

from store import load_items, save_items

router = APIRouter()


# -------- DATA MODELS --------

class RadioItem(BaseModel):
    title: str
    artist: str
    plays: int


class BulkRadio(BaseModel):
    items: List[RadioItem]


# -------- HELPERS --------

def find_song(items, title, artist):
    for item in items:
        if item["title"] == title and item["artist"] == artist:
            return item
    return None


# -------- ROUTE --------

@router.post("/ingest/radio")
def ingest_radio(payload: Union[RadioItem, BulkRadio]):
    items = load_items()
    ingested = 0

    # Normalize to list
    if isinstance(payload, BulkRadio):
        incoming = payload.items
    else:
        incoming = [payload]

    for entry in incoming:
        song = find_song(items, entry.title, entry.artist)

        if song:
            song["radio"] = song.get("radio", 0) + entry.plays
        else:
            items.append({
                "title": entry.title,
                "artist": entry.artist,
                "youtube": 0,
                "radio": entry.plays,
                "tv": 0,
                "score": 0,
            })

        ingested += 1

    save_items(items)

    return {
        "status": "ok",
        "ingested": ingested
    }