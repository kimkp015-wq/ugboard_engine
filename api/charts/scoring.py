import json
import os

INGESTION_FILE = "data/ingestion.json"


YOUTUBE_WEIGHT = 1
RADIO_WEIGHT = 3
TV_WEIGHT = 5


def load_ingestion():
    if not os.path.exists(INGESTION_FILE):
        return []

    with open(INGESTION_FILE, "r") as f:
        data = json.load(f)

    return data.get("songs", [])


def calculate_scores():
    songs = load_ingestion()
    scored = []

    for song in songs:
        youtube = song.get("youtube", 0)
        radio = song.get("radio", 0)
        tv = song.get("tv", 0)

        score = (
            youtube * YOUTUBE_WEIGHT +
            radio * RADIO_WEIGHT +
            tv * TV_WEIGHT
        )

        scored.append({
            "title": song["title"],
            "artist": song["artist"],
            "youtube": youtube,
            "radio": radio,
            "tv": tv,
            "score": score
        })

    scored.sort(key=lambda x: x["score"], reverse=True)

    return scored