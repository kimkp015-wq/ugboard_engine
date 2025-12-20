from fastapi import APIRouter
from api.storage import db
from api.charts.recalculate import recalculate_top100

router = APIRouter()


@router.post("/tv")
def ingest_tv(payload: dict):
    items = payload.get("items", [])

    for item in items:
        title = item["title"]
        artist = item["artist"]
        appearances = item.get("appearances", 0)

        score = appearances * 5  # tv weight

        db.top100.update_one(
            {"title": title, "artist": artist},
            {
                "$inc": {
                    "tv": appearances,
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