from fastapi import APIRouter
from typing import Dict, List, Union
from data.store import load_items, save_items

router = APIRouter()


@router.post("/tv")
def ingest_tv(payload: Union[Dict, List[Dict]]):
    """
    Accepts:
    - Single record
    - Bulk records

    Each record:
    {
      "title": "Song",
      "artist": "Artist",
      "plays": 10
    }
    """

    items = load_items()

    # Normalize payload to list
    if isinstance(payload, dict):
        payload = [payload]

    ingested = 0

    for record in payload:
        title = record.get("title")
        artist = record.get("artist")
        plays = int(record.get("plays", 0))

        if not title or not artist:
            continue

        song = next(
            (i for i in items if i.get("title") == title and i.get("artist") == artist),
            None
        )

        # Create song if missing
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

        song["tv"] += plays
        ingested += 1

    save_items(items)

    return {
        "status": "ok",
        "ingested": ingested
    }