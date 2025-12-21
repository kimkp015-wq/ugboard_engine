from fastapi import APIRouter
from data.store import load_items, save_items
from api.scoring.auto import safe_auto_recalculate

router = APIRouter()


@router.post("/ingest/tv")
def ingest_tv(payload: dict):
    items = load_items()

    records = payload.get("items")
    if not isinstance(records, list):
        records = [payload]

    ingested = 0

    for record in records:
        title = record.get("title")
        artist = record.get("artist")
        plays = int(record.get("plays", 0))

        for item in items:
            if item["title"] == title and item["artist"] == artist:
                item["tv"] = item.get("tv", 0) + plays
                ingested += 1
                break

    save_items(items)

    # 🔧 SAFE AUTO RECALC
    safe_auto_recalculate(items)

    return {
        "status": "ok",
        "ingested": ingested
    }