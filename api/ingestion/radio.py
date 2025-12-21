from fastapi import APIRouter
from typing import Dict, List, Union
from data.store import load_items, save_items
from api.charts.recalculate import safe_recalculate_top100

router = APIRouter()


@router.post("/radio")
def ingest_radio(payload: Union[Dict, List[Dict]]):
    items = load_items()

    records = payload if isinstance(payload, list) else payload.get("items", [payload])

    ingested = 0

    for record in records:
        title = record.get("title")
        artist = record.get("artist")
        plays = int(record.get("plays", 0))

        if not title or not artist:
            continue

        song = next(
            (i for i in items if i["title"] == title and i["artist"] == artist),
            None
        )

        if song:
            song["radio"] = song.get("radio", 0) + plays
            ingested += 1

    save_items(items)
    safe_recalculate_top100()

    return {
        "status": "ok",
        "ingested": ingested
    }