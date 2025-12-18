from datetime import datetime, timedelta

# In-memory daily counter (resets daily)
BOOST_LOG = {}

MAX_DAILY_BOOSTS = 10

def boost_song(song_id: str):
    # Use EAT (UTC+3)
    eat_now = datetime.utcnow() + timedelta(hours=3)
    today = eat_now.date().isoformat()

    if today not in BOOST_LOG:
        BOOST_LOG[today] = []

    if len(BOOST_LOG[today]) >= MAX_DAILY_BOOSTS:
        return {
            "status": "error",
            "message": "Daily boost limit reached (10)"
        }

    BOOST_LOG[today].append(song_id)

    return {
        "status": "ok",
        "boosted_song": song_id,
        "boosts_today": len(BOOST_LOG[today]),
        "remaining": MAX_DAILY_BOOSTS - len(BOOST_LOG[today]),
        "timezone": "EAT (UTC+3)",
        "date": today
    }