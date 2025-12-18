from api.charts.data import UG_TOP_SONGS

def build_trending():
    trending = []

    for index, song in enumerate(UG_TOP_SONGS[:5], start=1):
        trending.append({
            "rank": index,
            "artist": song["artist"],
            "title": song["title"]
        })

    return {
        "status": "ok",
        "chart": "Uganda Trending",
        "total": len(trending),
        "data": trending
    }