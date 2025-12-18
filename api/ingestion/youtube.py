import requests
import xml.etree.ElementTree as ET

HEADERS = {"User-Agent": "Mozilla/5.0"}

# YouTube Music Topic (public)
MUSIC_TOPIC_FEED = "https://www.youtube.com/feeds/videos.xml?topic_id=/m/04rlf"

# Curated Ugandan artist channels (we can expand later)
UG_ARTIST_CHANNELS = [
    "UCZzZx1z0y9zQJzvUgXcKJ6A",  # Eddy Kenzo (example)
    "UCyZx9GmQ5cZ5Q7LZzvUgXQ",  # Sheebah (example)
]

def parse_feed(xml_text):
    root = ET.fromstring(xml_text)
    ns = {
        "atom": "http://www.w3.org/2005/Atom",
        "yt": "http://www.youtube.com/xml/schemas/2015"
    }

    videos = []
    for entry in root.findall("atom:entry", ns):
        video_id = entry.find("yt:videoId", ns)
        title = entry.find("atom:title", ns)

        if video_id is None or title is None:
            continue

        videos.append({
            "video_id": video_id.text,
            "title": title.text,
            "url": f"https://www.youtube.com/watch?v={video_id.text}",
            "source": "YouTube"
        })

    return videos


def fetch_ugandan_music(max_results=20):
    collected = []

    # 1️⃣ Music topic feed
    try:
        r = requests.get(MUSIC_TOPIC_FEED, headers=HEADERS, timeout=10)
        if r.status_code == 200:
            collected.extend(parse_feed(r.text))
    except Exception:
        pass

    # 2️⃣ Ugandan artist channels
    for channel_id in UG_ARTIST_CHANNELS:
        try:
            r = requests.get(
                f"https://www.youtube.com/feeds/videos.xml?channel_id={channel_id}",
                headers=HEADERS,
                timeout=10
            )
            if r.status_code == 200:
                collected.extend(parse_feed(r.text))
        except Exception:
            continue

    # 3️⃣ Deduplicate
    unique = {}
    for v in collected:
        unique[v["video_id"]] = v

    results = list(unique.values())[:max_results]

    return {
        "status": "ok",
        "count": len(results),
        "data": results,
        "note": "Public RSS sources (free, no API)"
    }