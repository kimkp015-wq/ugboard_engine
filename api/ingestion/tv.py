from fastapi import APIRouter
from typing import List, Dict, Union
from data.store import load_items, save_items

router = APIRouter()


@router.post("/tv")
def ingest_tv(payload: Union[Dict, List[Dict]]):
    items = load_items()

    if isinstance(payload, dict):
        payload = [payload]

    ingested = 0

    for entry in payload:
        title = entry.get("title")
        artist = entry.get("artist")
        plays = int(entry.get("plays", 0))

        if not title or not artist:
            continue

        for item in items:
            if item["title"] == title and item["artist"] == artist:
                item["tv"] = item.get("tv", 0) + plays
                ingested += 1
                break

    save_items(items)

    return {
        "status": "ok",
        "ingested": ingested
    }