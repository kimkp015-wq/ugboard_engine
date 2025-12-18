from fastapi import FastAPI
import requests
import feedparser

app = FastAPI(title="UG Board Engine")

YOUTUBE_RSS_URL = "https://www.youtube.com/feeds/videos.xml?channel_id="

# Sample Ugandan music channels (can expand later)
UG_CHANNELS = {
    "Eddy Kenzo": "UCZzZx1z0y9zQJzvUgXcKJ6A",
    "Sheebah": "UCyZx9GmQ5cZ5Q7LZzvUgXQ",
    "Jose Chameleone": "UC1UgZChameleonOfficial",
}

@app.get("/")
def root():
    return {
        "engine": "UG Board",
        "status": "running"
    }

@app.get("/ingest/youtube")
def ingest_youtube():
    results = []

    for artist, channel_id in UG_CHANNELS.items():
        try:
            feed = feedparser.parse(YOUTUBE_RSS_URL + channel_id)

            for entry in feed.entries[:5]:
                results.append({
                    "artist": artist,
                    "title": entry.title,
                    "link": entry.link,
                    "published": entry.get("published", "")
                })

        except Exception:
            continue

    if not results:
        return {
            "status": "error",
            "message": "YouTube feed unavailable",
            "code": 400
        }

    return {
        "status": "ok",
        "source": "YouTube RSS",
        "count": len(results),
        "data": results
    }