from fastapi import APIRouter
from api.storage import db
from api.charts.recalculate import recalculate_top100

router = APIRouter()


@router.post("/youtube")
def ingest_youtube(payload: dict):
    items = payload.get("items", [])

    for item in items:
        title = item["title"]
        artist = item["artist"]
        views = item.get("views", 0)

        score = views  # youtube = 1x

        db.top100.update_one(
            {"title": title, "artist": artist},
            {
                "$inc": {
                    "youtube": views,
                    "score": score
                }
            },
            upsert=True
        )

    total = recalculate_top100()

    return {
        "status": "ok",
        "ingested": len(items),
        "recalculated": total
    }