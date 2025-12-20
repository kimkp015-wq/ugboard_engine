import json
import os

BOOST_FILE = "data/boosts.json"


def load_boosts():
    if not os.path.exists(BOOST_FILE):
        return []

    try:
        with open(BOOST_FILE, "r") as f:
            return json.load(f)
    except Exception:
        return []


def apply_boosts(items):
    boosts = load_boosts()

    if not boosts:
        return items

    boost_map = {}

    for b in boosts:
        title = b.get("title")
        artist = b.get("artist")
        points = b.get("points", 0)

        if not title or not artist:
            continue

        key = f"{title}|{artist}"
        boost_map[key] = points

    for song in items:
        key = f"{song.get('title')}|{song.get('artist')}"
        boost = boost_map.get(key, 0)

        song["boost"] = boost
        song["score"] += boost

    return items