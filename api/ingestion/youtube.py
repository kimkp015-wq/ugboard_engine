from fastapi import APIRouter
from api.schemas.ingestion import YouTubePayload
from data.store import (
    load_items,
    save_items,
    load_ingestion_log,
    save_ingestion_log
)
from api.scoring.auto_recalc import try_auto_recalculate

router = APIRouter()


@router.post("/ingest/youtube")
def ingest_youtube(payload: YouTubePayload):
    items = load_items()
    log = load_ingestion_log()

    ingested = 0

    for entry in payload.items:
        key = f"youtube|{entry.title.lower()}|{entry.artist.lower()}|{entry.views}"

        if key in log:
            continue  # already processed

        song = next(
            (i for i in items if i["title"] == entry.title and i["artist"] == entry.artist),
            None
        )

        if not song:
            song = {
                "title": entry.title,
                "artist": entry.artist,
                "youtube": 0,
                "radio": 0,
                "tv": 0,
                "score": 0
            }
            items.append(song)

        song["youtube"] += entry.views
        log.add(key)
        ingested += 1

    save_items(items)
    save_ingestion_log(log)
    try_auto_recalculate()

    return {
        "status": "ok",
        "ingested": ingested,
        "deduplicated": len(payload.items) - ingested
    }