# api/ingestion/tv.py

from fastapi import APIRouter, BackgroundTasks
from typing import Dict, List, Union

from data.store import load_items, save_items
from api.scoring.auto_recalc import safe_auto_recalculate, mark_ingestion

router = APIRouter()


@router.post("/tv", operation_id="ingest_tv")
def ingest_tv(
    payload: Union[Dict, List[Dict]],
    background_tasks: BackgroundTasks
):
    items = load_items()

    # Normalize payload
    if isinstance(payload, dict):
        records = payload.get("items", [payload])
    else:
        records = payload

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

        # TV does NOT auto-create songs (YouTube is the source of truth)
        if not song:
            continue

        song["tv"] = song.get("tv", 0) + plays
        ingested += 1

    save_items(items)

    # Safe auto-recalculation (debounced, background)
    mark_ingestion()
    background_tasks.add_task(safe_auto_recalculate)

    return {
        "status": "ok",
        "ingested": ingested
    }