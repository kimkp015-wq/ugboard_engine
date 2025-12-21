from fastapi import APIRouter, BackgroundTasks
from data.store import load_items, save_items
from api.scoring.auto_recalc import safe_auto_recalculate, mark_ingestion

router = APIRouter()


@router.post("/radio")
def ingest_radio(payload: dict, background_tasks: BackgroundTasks):
    items = load_items()

    records = payload.get("items")
    if not isinstance(records, list):
        records = [payload]

    ingested = 0

    for record in records:
        title = record.get("title")
        artist = record.get("artist")
        plays = int(record.get("plays", 0))

        if not title or not artist:
            continue

        for item in items:
            if item["title"] == title and item["artist"] == artist:
                item["radio"] = item.get("radio", 0) + plays
                ingested += 1
                break

    save_items(items)

    mark_ingestion()
    background_tasks.add_task(safe_auto_recalculate)

    return {"status": "ok", "ingested": ingested}