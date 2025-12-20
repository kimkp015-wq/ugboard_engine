from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import List, Optional
import json
import os

router = APIRouter()

TOP100_PATH = "api/data/top100.json"


# ---------- MODELS ----------

class YoutubeItem(BaseModel):
    title: str
    artist: str
    views: int


class BulkYoutubePayload(BaseModel):
    items: List[YoutubeItem]


# ---------- HELPERS ----------

def load_top100():
    if not os.path.exists(TOP100_PATH):
        return {"items": []}

    with open(TOP100_PATH, "r") as f:
        return json.load(f)


def save_top100(data):
    with open(TOP100_PATH, "w") as f:
        json.dump(data, f, indent=2)


def ingest_one(item: YoutubeItem, data):
    for song in data["items"]:
        if song["title"] == item.title and song["artist"] == item.artist:
            song["youtube"] = song.get("youtube", 0) + item.views
            return

    # If song not found, create it
    data["items"].append({
        "title": item.title,
        "artist": item.artist,
        "youtube": item.views,
        "radio": 0,
        "tv": 0,
        "score": 0
    })


# ---------- ROUTE ----------

@router.post("/ingest/youtube")
def ingest_youtube(payload: dict):
    data = load_top100()

    # BULK MODE
    if "items" in payload:
        if not isinstance(payload["items"], list):
            raise HTTPException(status_code=400, detail="items must be a list")

        for raw in payload["items"]:
            item = YoutubeItem(**raw)
            ingest_one(item, data)

        save_top100(data)

        return {
            "status": "ok",
            "ingested": len(payload["items"]),
            "mode": "bulk"
        }

    # SINGLE MODE
    try:
        item = YoutubeItem(**payload)
    except Exception as e:
        raise HTTPException(status_code=422, detail=str(e))

    ingest_one(item, data)
    save_top100(data)

    return {
        "status": "ok",
        "ingested": 1,
        "mode": "single"
    }