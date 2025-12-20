from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import List

from data.store import load_top100, save_top100

router = APIRouter()


class PublishItem(BaseModel):
    title: str
    artist: str


class PublishPayload(BaseModel):
    items: List[PublishItem]


@router.post("/publish")
def publish_top100(payload: PublishPayload):
    data = load_top100()

    items = []
    for index, item in enumerate(payload.items, start=1):
        items.append({
            "position": index,
            "title": item.title,
            "artist": item.artist,
            "youtube": 0,
            "radio": 0,
            "tv": 0,
            "score": 0
        })

    data["items"] = items
    save_top100(data)

    return {
        "status": "ok",
        "published": len(items)
    }