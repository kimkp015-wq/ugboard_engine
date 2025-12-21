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
    """
    TV ingestion:
    - Accepts single object OR bulk list
    - Updates ONLY existing songs
    - Never creates new songs
    - Never crashes
    """

    items = load_items()

    # Normalize payload to list
    if isinstance(payload, dict):
        records = payload.get("items", payload)
        if isinstance(records, dict):
            records = [records]
    else:
        records = payload

    if not isinstance(records, list):
        return {"status": "ok", "ingested": 0}

    ingested = 0

    for record in records:
        title = record.get("title")
        artist = record.get("artist")
        plays = int(record.get("plays", 0))

        if not title or not artist:
            continue

        for item in items:
            if item.get("title") == title and item.get("artist") == artist:
                item["tv"] = int(item.get("tv", 0)) + plays
                ingested += 1
                break

    save_items(items)

    # SAFE auto-recalculate (debounced, background)
    mark_ingestion()
    background_tasks.add_task(safe_auto_recalculate)

    return {
        "status": "ok",
        "ingested": ingested
    }