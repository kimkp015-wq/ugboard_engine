from fastapi import APIRouter, HTTPException
import json
import os

router = APIRouter()

DATA_FILE = "data/top100.json"


@router.get("/charts/top100")
def get_top100():
    if not os.path.exists(DATA_FILE):
        raise HTTPException(status_code=404, detail="Top 100 not published")

    with open(DATA_FILE, "r") as f:
        data = json.load(f)

    items = data.get("items", [])

    scored_items = []
    for item in items:
        position = item.get("position")
        score = 101 - position

        scored_items.append({
            **item,
            "score": score
        })

    return {
        "status": "ok",
        "updated_at": data.get("updated_at"),
        "count": len(scored_items),
        "items": scored_items
    }