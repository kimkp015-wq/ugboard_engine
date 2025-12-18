from api.charts.data import UG_TOP_SONGS
import random

def build_top_100():
    ranked = []

    for index, song in enumerate(UG_TOP_SONGS):
        youtube_signal = random.randint(20, 60)
        radio_signal = random.randint(10, 40)
        social_signal = random.randint(15, 50)

        score = youtube_signal + radio_signal + social_signal

        ranked.append({
            "rank": index + 1,
            "artist": song.get("artist"),
            "title": song.get("title"),
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
        "total": len(ranked),
        "data": ranked
    }