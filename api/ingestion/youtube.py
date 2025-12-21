from fastapi import APIRouter
from typing import List, Dict, Union
from data.store import load_items, save_items

router = APIRouter()

@router.post("/ingest/youtube")
def ingest_youtube(payload: Union[Dict, List[Dict]]):
    items = load_items()

    if isinstance(payload, dict):
        payload = [payload]

    ingested = 0

    for entry in payload:
        title = entry.get("title")
        artist = entry.get("artist")
        views = int(entry.get("views", 0))

        if not title or not artist:
            continue

        song = next(
            (i for i in items if i["title"] == title and i["artist"] == artist),
            None
        )

        if not song:
            song = {
                "title": title,
                "artist": artist,
                "youtube": 0,
                "radio": 0,
                "tv": 0,
                "score": 0
            }
            items.append(song)

        song["youtube"] += views
        ingested += 1

    save_items(items)

    return {
        "status": "ok",
        "ingested": ingested
    }