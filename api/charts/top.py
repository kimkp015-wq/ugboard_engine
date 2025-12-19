from fastapi import APIRouter
import json
import os

router = APIRouter()

DATA_FILE = "data/top100.json"


@router.get("/charts/top100")
def get_top100():
    # If chart not published yet
    if not os.path.exists(DATA_FILE):
        return {
            "status": "ok",
            "count": 0,
            "items": []
        }

    # Read stored chart
    with open(DATA_FILE, "r") as f:
        data = json.load(f)

    items = data.get("items", [])

    final_items = []

    for item in items:
        youtube = item.get("youtube", 0)
        radio = item.get("radio", 0)
        tv = item.get("tv", 0)

        score = youtube + radio + tv

        final_items.append({
            "position": item["position"],
            "title": item["title"],
            "artist": item["artist"],
            "youtube": youtube,
            "radio": radio,
            "tv": tv,
            "score": score
        })

    return {
        "status": "ok",
        "count": len(final_items),
        "items": final_items
    }