from api.charts.data import UG_TOP_SONGS


def calculate_score(song):
    streams = song.get("streams_score", 0)
    radio = song.get("radio_score", 0)
    admin = song.get("admin_boost", 0)
    return streams + radio + admin


def build_top_100():
    scored_songs = []

    for song in UG_TOP_SONGS:
        score = calculate_score(song)
        song_copy = song.copy()
        song_copy["score"] = score
        scored_songs.append(song_copy)

    # sort by score (highest first)
    scored_songs.sort(key=lambda x: x["score"], reverse=True)

    # assign ranks
    for index, song in enumerate(scored_songs, start=1):
        song["rank"] = index

    return {
        "status": "ok",
        "chart": "Uganda Top 100",
        "scoring": "streams + radio + admin_boost",
        "total": len(scored_songs),
        "data": scored_songs
    }