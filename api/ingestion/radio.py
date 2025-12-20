from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import List
import json
import os

router = APIRouter()


DATA_PATHS = [
    "api/data/top100.json",
    "data/top100.json",
    "ingestion/top100.json",
    "/app/api/data/top100.json",
    "/app/data/top100.json",
    "/app/ingestion/top100.json",
]


def resolve_top100_path():
    for path in DATA_PATHS:
        if os.path.exists(path):
            return path
    return None


# ---------- MODELS ----------

class RadioItem(BaseModel):
    title: str
    artist: str
    plays: int


class RadioBulk(BaseModel):
    items: List[RadioItem]


# ---------- ROUTE ----------

@router.post("/radio")
def ingest_radio(payload: RadioBulk):
    path = resolve_top100_path()

    if not path:
        raise HTTPException(status_code=500, detail="Top100 file not found")

    with open(path, "r") as f:
        data = json.load(f)

    items = data.get("items", [])
    ingested = 0

    for entry in payload.items:
        for song in items:
            if (
                song.get("title") == entry.title
                and song.get("artist") == entry.artist
            ):
                song["radio"] = song.get("radio", 0) + entry.plays
                ingested += 1

    with open(path, "w") as f:
        json.dump(data, f, indent=2)

    return {
        "status": "ok",
        "ingested": ingested
    }