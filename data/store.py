import json
import os

DATA_FILE = "data/ingestion.json"


def load_ingestion():
    if not os.path.exists(DATA_FILE):
        return {"songs": []}

    try:
        with open(DATA_FILE, "r") as f:
            return json.load(f)
    except Exception:
        return {"songs": []}


def save_ingestion(data):
    os.makedirs("data", exist_ok=True)
    with open(DATA_FILE, "w") as f:
        json.dump(data, f, indent=2)


def update_song(title, artist, source):
    data = load_ingestion()
    songs = data.get("songs", [])

    for song in songs:
        if song["title"] == title and song["artist"] == artist:
            song[source] = song.get(source, 0) + 1
            save_ingestion(data)
            return

    songs.append({
        "title": title,
        "artist": artist,
        "youtube": 0,
        "radio": 0,
        "tv": 0,
        source: 1
    })

    data["songs"] = songs
    save_ingestion(data)