import math

# Scoring weights (constants)
YOUTUBE_WEIGHT = 1.0
RADIO_WEIGHT = 8.0
TV_WEIGHT = 12.0
BOOST_WEIGHT = 1.0


def safe_int(value):
    try:
        return int(value)
    except Exception:
        return 0


def compute_score(song: dict) -> int:
    youtube = safe_int(song.get("youtube", 0))
    radio = safe_int(song.get("radio", 0))
    tv = safe_int(song.get("tv", 0))
    boost = safe_int(song.get("boost", 0))

    score = (
        youtube * YOUTUBE_WEIGHT +
        radio * RADIO_WEIGHT +
        tv * TV_WEIGHT +
        boost * BOOST_WEIGHT
    )

    return math.floor(score)


def score_all_songs(ingestion_data: dict) -> list:
    songs = ingestion_data.get("songs", [])
    results = []

    for song in songs:
        results.append({
            "id": song.get("id"),
            "title": song.get("title", "").strip(),
            "artist": song.get("artist", "").strip(),
            "youtube": safe_int(song.get("youtube", 0)),
            "radio": safe_int(song.get("radio", 0)),
            "tv": safe_int(song.get("tv", 0)),
            "boost": safe_int(song.get("boost", 0)),
            "score": compute_score(song)
        })

    return results