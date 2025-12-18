from api.charts.data import UG_TOP_SONGS
import random
from datetime import datetime

def build_top_100():
    ranked = []

    for index, song in enumerate(UG_TOP_SONGS, start=1):
        youtube_signal = random.randint(20, 100)
        radio_signal = random.randint(10, 80)
        social_signal = random.randint(15, 90)

        score = youtube_signal + radio_signal + social_signal

        ranked.append({
            "rank": index,
            "artist": song["artist"],
            "title": song["title"],
            "score": score,
            "signals": {
                "youtube": youtube_signal,
                "radio": radio_signal,
                "social": social_signal,
                "updated_at": datetime.utcnow().isoformat()
            }
        })

    return {
        "status": "ok",
        "chart": "Uganda Top 100",
        "total": len(ranked),
        "data": ranked
    }