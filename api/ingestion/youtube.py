# api/ingestion/youtube.py

from fastapi import APIRouter
from typing import Union, List, Dict
from data.store import load_items, save_items
from api.scoring.scoring import recalculate_all

# SAFE OPTIONAL LOGGING
try:
    from data.ingestion_log import log_ingestion
except Exception:
    log_ingestion = None

router = APIRouter()


@router.post("/youtube")
def ingest_youtube(payload: Union[Dict, List[Dict]]):
    items = load_items()

    # Normalize payload to list
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

    # Auto recalculate safely
    items = recalculate_all(items)
    save_items(items)

    # Log ingestion safely
    if log_ingestion:
        log_ingestion("youtube", ingested, payload)

    return {
        "status": "ok",
        "ingested": ingested
    }