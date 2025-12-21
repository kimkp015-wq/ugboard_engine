from fastapi import APIRouter
from data.store import load_items, save_items
from api.schemas.ingestion import RadioPayload
from api.scoring.auto_recalc import try_auto_recalculate

router = APIRouter()


@router.post("/ingest/radio")
def ingest_radio(payload: RadioPayload):
    items = load_items()
    ingested = 0

    for entry in payload.items:
        for song in items:
            if song["title"] == entry.title and song["artist"] == entry.artist:
                song["radio"] += entry.plays
                ingested += 1
                break

    save_items(items)
    try_auto_recalculate()

    return {
        "status": "ok",
        "ingested": ingested
    }