from fastapi import APIRouter
from data.store import (
    load_items,
    save_items,
    load_ingestion_log,
    save_ingestion_log
)
from api.scoring.auto_recalc import try_auto_recalculate

router = APIRouter()


@router.post("/ingest/tv")
def ingest_tv(payload: dict):
    """
    Expected payload:
    {
        "items": [
            { "title": "...", "artist": "...", "plays": 5 }
        ]
    }
    """

    items = load_items()
    log = load_ingestion_log()

    records = payload.get("items", [])
    if not isinstance(records, list):
        records = [payload]

    ingested = 0

    for record in records:
        title = record.get("title")
        artist = record.get("artist")
        plays = int(record.get("plays", 0))

        if not title or not artist or plays <= 0:
            continue

        key = f"tv|{title.lower()}|{artist.lower()}|{plays}"

        # Deduplication guard
        if key in log:
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

        song["tv"] += plays
        log.add(key)
        ingested += 1

    save_items(items)
    save_ingestion_log(log)
    try_auto_recalculate()

    return {
        "status": "ok",
        "ingested": ingested,
        "skipped": len(records) - ingested
    }