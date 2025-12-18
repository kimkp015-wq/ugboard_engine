# api/charts/top100.py

from api.charts.data import UG_TOP_SONGS
from datetime import datetime

def build_top_100():
    ranked = []

    for index, song in enumerate(UG_TOP_SONGS, start=1):
        ranked.append({
            "rank": index,
            "artist": song["artist"],
            "title": song["title"],
            "last_updated": datetime.utcnow().isoformat()
        })

    return {
        "status": "ok",
        "chart": "Uganda Top 100",
        "total": len(ranked),
        "data": ranked
    }