from fastapi import APIRouter
import json
import os
from datetime import datetime

router = APIRouter()

YOUTUBE_FILE = "data/youtube.json"


@router.post("/ingestion/youtube")
def ingest_youtube(payload: dict):
    """
    Expected payload example:
    {
      "title": "Test Song",
      "artist": "Test Artist",
      "views": 12000
    }
    """

    title = payload.get("title")
    artist = payload.get("artist")
    views = payload.get("views", 0)

    if not title or not artist:
        return {"status": "error", "detail": "title and artist required"}

    os.makedirs("data", exist_ok=True)

    if os.path.exists(YOUTUBE_FILE):
        with open(YOUTUBE_FILE, "r") as f:
            data = json.load(f)
    else:
        data = {"updated_at": None, "items": []}

    # Update or insert
    found = False
    for item in data["items"]:
        if item["title"] == title and item["artist"] == artist:
            item["views"] += int(views)
            found = True
            break

    if not found:
        data["items"].append({
            "title": title,
            "artist": artist,
            "views": int(views)
        })

    data["updated_at"] = datetime.utcnow().isoformat()

    with open(YOUTUBE_FILE, "w") as f:
        json.dump(data, f, indent=2)

    return {
        "status": "ok",
        "title": title,
        "artist": artist,
        "views_added": views
    }