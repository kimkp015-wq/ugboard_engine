from fastapi import APIRouter
from data.store import load_items, save_items
from api.scoring.scoring import recalculate_all

router = APIRouter()

@router.post("/ingest/tv")
def ingest_tv(payload: dict):
    items = load_items()

    records = payload.get("items", [payload])
    ingested = 0

    for record in records:
        title = record.get("title")
        artist = record.get("artist")
        plays = int(record.get("plays", 0))

        for item in items:
            if item["title"] == title and item["artist"] == artist:
                item["tv"] += plays
                ingested += 1
                break

    items = recalculate_all(items)
    save_items(items)

    return {
        "status": "ok",
        "ingested": ingested
    }