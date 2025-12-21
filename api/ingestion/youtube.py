from fastapi import APIRouter
from typing import List, Dict, Union
from data.store import load_items, save_items

router = APIRouter()


@router.post("/youtube")
def ingest_youtube(payload: Union[Dict, List[Dict]]):
    """
    Accepts:
    - Single item (dict)
    - Bulk items (list of dicts)

    Each item:
    {
      "title": "Song",
      "artist": "Artist",
      "views": 100
    }
    """

    items = load_items()

    # Normalize to list
    if isinstance(payload, dict):
        payload = [payload]

    ingested = 0

    for entry in payload:
        title = entry.get("title")
        artist = entry.get("artist")
        views = int(entry.get("views", 0))

        if not title or not artist:
            continue

        # Find existing song
        song = next(
            (i for i in items if i.get("title") == title and i.get("artist") == artist),
            None
        )

        # Create if missing
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