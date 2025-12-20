from fastapi import APIRouter, HTTPException
from api.scoring.scoring import calculate_score
import json
import os

router = APIRouter()


def resolve_top100_path():
    candidates = [
        "ingestion/top100.json",
        "data/top100.json",
        "/app/ingestion/top100.json",
        "/app/data/top100.json",
    ]

    for path in candidates:
        if os.path.exists(path):
            return path

    return None


@router.post("/youtube")
def ingest_youtube(payload: dict):
    """
    Expected payload:
    {
      "title": "Song",
      "artist": "Artist",
      "views": 12345
    }
    """

    title = payload.get("title")
    artist = payload.get("artist")
    views = payload.get("views", 0)

    if not title or not artist:
        raise HTTPException(status_code=400, detail="title and artist required")

    path = resolve_top100_path()
    if not path:
        raise HTTPException(status_code=500, detail="Top100 file not found")

    try:
        with open(path, "r") as f:
            data = json.load(f)
    except Exception:
        data = {"items": []}

    items = data.get("items", [])

    # Find or create entry
    item = None
    for i in items:
        if i.get("title") == title and i.get("artist") == artist:
            item = i
            break

    if not item:
        item = {
            "title": title,
            "artist": artist,
            "youtube": 0,
            "radio": 0,
            "tv": 0,
            "score": 0
        }
        items.append(item)

    # Update YouTube value
    item["youtube"] = max(0, int(views))

    # Recalculate score
    item["score"] = calculate_score(
        youtube=item.get("youtube", 0),
        radio=item.get("radio", 0),
        tv=item.get("tv", 0),
    )

    data["items"] = items

    with open(path, "w") as f:
        json.dump(data, f, indent=2)

    return {
        "status": "youtube_ingested",
        "title": title,
        "artist": artist,
        "score": item["score"]
    }