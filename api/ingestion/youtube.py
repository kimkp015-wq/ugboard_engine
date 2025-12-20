from fastapi import APIRouter
from store import load_items, save_items
from api.scoring.scoring import recalculate_all

router = APIRouter()


@router.post("/ingest/youtube")
def ingest_youtube(payload: dict):
    items = load_items()

    records = payload.get("items")
    if not isinstance(records, list):
        records = [payload]

    ingested = 0

    for record in records:
        title = record.get("title")
        artist = record.get("artist")
        views = int(record.get("views", 0))

        for item in items:
            if item["title"] == title and item["artist"] == artist:
                item["youtube"] = item.get("youtube", 0) + views
                ingested += 1
                break

    # ✅ AUTO RECALCULATE
    items = recalculate_all(items)

    save_items(items)

    return {
        "status": "ok",
        "ingested": ingested
    }