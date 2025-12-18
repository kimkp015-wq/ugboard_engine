from api.charts.data import UG_TOP_SONGS
import random

def build_top_100():
    ranked = []

    for index, song in enumerate(UG_TOP_SONGS, start=1):
        youtube = random.randint(20, 100)
        radio = random.randint(10, 80)
        social = random.randint(15, 90)
        source_bonus = len(song.get("sources", [])) * 50

        score = youtube + radio + social + source_bonus

        ranked.append({
            "rank": index,
            "artist": song["artist"],
            "title": song["title"],
            "score": score,
            "sources": song.get("sources", [])
        })

    return {
        "status": "ok",
        "chart": "Uganda Top 100",
        "data": ranked
    }