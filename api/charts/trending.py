from fastapi import APIRouter
from datetime import datetime, timedelta
import random

router = APIRouter()

UG_TOP_SONGS = [
    {"artist": "Artist A", "title": "Song A"},
    {"artist": "Artist B", "title": "Song B"},
]

@router.get("/trending")
def trending_today():
    eat_now = datetime.utcnow() + timedelta(hours=3)
    today = eat_now.date().isoformat()

    random.seed(today)
    trending = []

    for song in UG_TOP_SONGS:
        score = random.randint(20, 100)
        trending.append({
            "artist": song["artist"],
            "title": song["title"],
            "trend_score": score
        })

    trending.sort(key=lambda x: x["trend_score"], reverse=True)

    return {
        "status": "ok",
        "chart": "Uganda Trending Songs",
        "timezone": "EAT (UTC+3)",
        "locked_for": today,
        "data": trending[:20]
    }