from fastapi import APIRouter
from typing import List, Dict, Union
from data.store import load_items, save_items
from api.charts.recalculate import safe_recalculate_top100

router = APIRouter()


@router.post("/youtube")
def ingest_youtube(payload: Union[Dict, List[Dict]]):
    items = load_items()

    # normalize to list
    records = payload if isinstance(payload, list) else [payload]

    ingested = 0

    for record in records:
        title = record.get("title")
        artist = record.get("artist")
        views = int(record.get("views", 0))

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
    safe_recalculate_top100()

    return {
        "status": "ok",
        "ingested": ingested
    }