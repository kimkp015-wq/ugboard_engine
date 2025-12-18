from datetime import datetime, timedelta
import random

REGIONS = {
    "kampala": "Kampala",
    "central": "Central Uganda",
    "eastern": "Eastern Uganda",
    "western": "Western Uganda",
    "northern": "Northern Uganda"
}

def build_regional_chart(region_key):
    if region_key not in REGIONS:
        return {"status": "error", "message": "Region not supported"}

    # Lock daily using EAT (UTC+3)
    eat_now = datetime.utcnow() + timedelta(hours=3)
    today = eat_now.date().isoformat()
    random.seed(region_key + today)

    songs = []
    for i in range(1, 51):
        songs.append({
            "rank": i,
            "title": f"Song {i}",
            "artist": f"Artist {i}",
            "score": random.randint(40, 120)
        })

    return {
        "status": "ok",
        "region": REGIONS[region_key],
        "chart": "Top 50",
        "locked_for": today,
        "timezone": "EAT (UTC+3)",
        "data": songs
    }