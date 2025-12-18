from api.charts.data import UG_TOP_SONGS
import random
from datetime import datetime, timedelta

def build_trending():
    # Convert UTC to EAT (UTC +3)
    eat_now = datetime.utcnow() + timedelta(hours=3)
    today = eat_now.date().isoformat()

    random.seed(today)  # 🔒 lock scores for EAT day

    trending = []

    for song in UG_TOP_SONGS:
        score = (
            random.randint(20, 60) +   # YouTube signal
            random.randint(10, 40) +   # Radio signal
            random.randint(5, 30)      # Social signal
        )

        trending.append({
            "artist": song["artist"],
            "title": song["title"],
            "trend_score": score
        })

    trending = sorted(trending, key=lambda x: x["trend_score"], reverse=True)[:20]

    return {
        "status": "ok",
        "chart": "Uganda Trending Songs",
        "locked_for": today,
        "timezone": "EAT (UTC+3)",
        "total": len(trending),
        "data": trending
    }