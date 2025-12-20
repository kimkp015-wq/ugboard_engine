from fastapi import APIRouter
from api.storage import db
from api.charts.recalculate import recalculate_top100

router = APIRouter()


@router.post("/radio")
def ingest_radio(payload: dict):
    items = payload.get("items", [])

    for item in items:
        title = item["title"]
        artist = item["artist"]
        plays = item.get("plays", 0)

        score = plays * 3  # radio weight

        db.top100.update_one(
            {"title": title, "artist": artist},
            {
                "$inc": {
                    "radio": plays,
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