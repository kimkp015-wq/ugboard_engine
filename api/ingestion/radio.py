# api/ingestion/radio.py

from fastapi import APIRouter
from data.store import load_items, save_items
from api.scoring.scoring import recalculate_all

# SAFE OPTIONAL LOGGING
try:
    from data.ingestion_log import log_ingestion
except Exception:
    log_ingestion = None

router = APIRouter()


@router.post("/radio")
def ingest_radio(payload: dict):
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

    # Auto recalculate safely
    items = recalculate_all(items)
    save_items(items)

    # Log ingestion safely
    if log_ingestion:
        log_ingestion("radio", ingested, records)

    return {
        "status": "ok",
        "ingested": ingested
    }