from api.charts.data import UG_TOP_SONGS
from api.charts.admin import ADMIN_BOOST
import random
from datetime import datetime, timedelta

def build_top_100():
    eat_now = datetime.utcnow() + timedelta(hours=3)
    today = eat_now.date().isoformat()

    random.seed(today)

    ranked = []

    for index, song in enumerate(UG_TOP_SONGS, start=1):
        youtube_signal = random.randint(20, 60)
        radio_signal = random.randint(10, 40)
        social_signal = random.randint(15, 50)

        base_score = youtube_signal + radio_signal + social_signal

        key = f"{song['artist']} - {song['title']}"
        boost = ADMIN_BOOST.get(key, 0)

        final_score = base_score + boost

        ranked.append({
            "rank": index,
            "artist": song["artist"],
            "title": song["title"],
            "score": final_score,
            "boost": boost,
            "signals": {
                "youtube": youtube_signal,
                "radio": radio_signal,
                "social": social_signal
            }
        })

    ranked = sorted(ranked, key=lambda x: x["score"], reverse=True)

    for i, song in enumerate(ranked, start=1):
        song["rank"] = i

    return {
        "status": "ok",
        "chart": "Uganda Top 100",
        "locked_for": today,
        "timezone": "EAT (UTC+3)",
        "boosts_active": True,
        "data": ranked
    }