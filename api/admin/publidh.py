import json
import os
from fastapi import APIRouter, HTTPException, Query

router = APIRouter(prefix="/admin", tags=["Admin"])

DATA_PATH = "data/top100.json"
ADMIN_KEY = os.getenv("ADMIN_SECRET_KEY")


def build_top100():
    songs = []
    for i in range(1, 101):
        songs.append({
            "rank": i,
            "title": f"UG Song {i}",
            "artist": "Ugandan Artist",
            "score": round(100 - (i * 0.5), 2)
        })
    return songs


@router.post("/publish/top100")
def publish_top100(key: str = Query(...)):
    if not ADMIN_KEY or key != ADMIN_KEY:
        raise HTTPException(status_code=403, detail="Unauthorized")

    os.makedirs("data", exist_ok=True)

    chart = {
        "chart": "UG Board Top 100",
        "count": 100,
        "data": build_top100()
    }

    with open(DATA_PATH, "w") as f:
        json.dump(chart, f, indent=2)

    return {"status": "published", "count": 100}