from fastapi import APIRouter
from data.store import load_items, save_items
from api.schemas.ingestion import YouTubePayload
from api.scoring.auto_recalc import try_auto_recalculate

router = APIRouter()


@router.post("/ingest/youtube")
def ingest_youtube(payload: YouTubePayload):
    items = load_items()
    ingested = 0

    for entry in payload.items:
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
        ingested += 1

    save_items(items)
    try_auto_recalculate()

    return {
        "status": "ok",
        "ingested": ingested
    }