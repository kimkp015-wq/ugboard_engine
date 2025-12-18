from fastapi import APIRouter
from datetime import datetime, timedelta

router = APIRouter()

BOOST_LOG = {}
MAX_DAILY_BOOSTS = 10

@router.post("/boost/{song_id}")
def boost_song(song_id: str):
    eat_now = datetime.utcnow() + timedelta(hours=3)
    today = eat_now.date().isoformat()

    if today not in BOOST_LOG:
        BOOST_LOG[today] = []

    if len(BOOST_LOG[today]) >= MAX_DAILY_BOOSTS:
        return {
            "status": "error",
            "message": "Daily boost limit reached",
            "timezone": "EAT (UTC+3)",
            "date": today
        }

    BOOST_LOG[today].append(song_id)

    return {
        "status": "ok",
        "boosted_song": song_id,
        "boosts_today": len(BOOST_LOG[today]),
        "remaining": MAX_DAILY_BOOSTS - len(BOOST_LOG[today]),
        "timezone": "EAT (UTC+3)"
    }