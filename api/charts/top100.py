from api.charts.data import UG_TOP_SONGS
import random
from datetime import datetime, timedelta

def build_top_100():
    # Convert UTC to EAT (UTC +3)
    eat_now = datetime.utcnow() + timedelta(hours=3)
    today = eat_now.date().isoformat()

    random.seed(today)  # 🔒 lock scores for the day (EAT)

    ranked = []

    for index, song in enumerate(UG_TOP_SONGS, start=1):
        youtube_signal = random.randint(20, 60)
        radio_signal = random.randint(10, 40)
        social_signal = random.randint(15, 50)

        score = youtube_signal + radio_signal + social_signal

        ranked.append({
            "rank": index,
            "artist": song["artist"],
            "title": song["title"],
            "score": score,
            "signals": {
                "youtube": youtube_signal,
                "radio": radio_signal,
                "social": social_signal
            }
        })

    return {
        "status": "ok",
        "chart": "Uganda Top 100",
        "locked_for": today,
        "timezone": "EAT (UTC+3)",
        "total": len(ranked),
        "data": ranked
    }