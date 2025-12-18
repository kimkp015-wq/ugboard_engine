from api.ingestion.youtube import fetch_ugandan_music


def build_top_100():
    response = fetch_ugandan_music(50)

    if response.get("status") != "ok":
        return response

    songs = response.get("results", [])

    ranked = []
    rank = 1

    for song in songs[:100]:
        ranked.append({
            "rank": rank,
            "title": song.get("title"),
            "channel": song.get("channel"),
        })
        rank += 1

    return {
        "status": "ok",
        "chart": "UG Board Top 100",
        "count": len(ranked),
        "data": ranked
    }