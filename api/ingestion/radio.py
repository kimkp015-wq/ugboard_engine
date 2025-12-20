from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
import json
import os

from api.scoring.scoring import calculate_score

router = APIRouter(prefix="/radio", tags=["ingestion"])


class RadioIngest(BaseModel):
    title: str
    artist: str
    plays: int = 1


def resolve_top100_path():
    candidates = [
        "api/data/top100.json",
        "data/top100.json",
        "ingestion/top100.json",
        "/app/api/data/top100.json",
        "/app/data/top100.json",
        "/app/ingestion/top100.json",
    ]

    for path in candidates:
        if os.path.exists(path):
            return path

    return None


@router.post("")
def ingest_radio(payload: RadioIngest):
    path = resolve_top100_path()

    if not path:
        raise HTTPException(status_code=500, detail="Top100 file not found")

    try:
        with open(path, "r") as f:
            data = json.load(f)
    except Exception:
        raise HTTPException(status_code=500, detail="Failed to read Top100")

    items = data.get("items", [])
    updated = False

    for item in items:
        if (
            item.get("title") == payload.title
            and item.get("artist") == payload.artist
        ):
            item["radio"] = int(item.get("radio", 0)) + payload.plays
            item["score"] = calculate_score(item)
            updated = True
            break

    if not updated:
        raise HTTPException(status_code=404, detail="Song not found in Top100")

    data["items"] = items

    try:
        with open(path, "w") as f:
            json.dump(data, f, indent=2)
    except Exception:
        raise HTTPException(status_code=500, detail="Failed to write Top100")

    return {
        "status": "ok",
        "message": "Radio plays added",
        "title": payload.title,
        "artist": payload.artist,
    }