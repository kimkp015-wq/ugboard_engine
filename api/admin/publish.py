from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, field_validator
from typing import List
from datetime import datetime
import json
import os

router = APIRouter()

DATA_FILE = "data/top100.json"


class Top100Item(BaseModel):
    position: int
    title: str
    artist: str

    @field_validator("position")
    @classmethod
    def position_range(cls, v):
        if v < 1 or v > 100:
            raise ValueError("position must be between 1 and 100")
        return v

    @field_validator("title", "artist")
    @classmethod
    def not_empty(cls, v):
        if not v or not v.strip():
            raise ValueError("title and artist cannot be empty")
        return v.strip()


class PublishTop100(BaseModel):
    items: List[Top100Item]

    @field_validator("items")
    @classmethod
    def validate_items(cls, items):
        if not items:
            raise ValueError("Top100 cannot be empty")

        if len(items) > 100:
            raise ValueError("Maximum 100 songs allowed")

        positions = [item.position for item in items]
        if len(set(positions)) != len(positions):
            raise ValueError("Duplicate chart positions are not allowed")

        return items


@router.post("/publish/top100")
def publish_top100(payload: PublishTop100):
    os.makedirs("data", exist_ok=True)

    # sort by position (1 → 100)
    sorted_items = sorted(payload.items, key=lambda x: x.position)

    data = {
        "updated_at": datetime.utcnow().isoformat(),
        "items": [item.model_dump() for item in sorted_items]
    }

    with open(DATA_FILE, "w") as f:
        json.dump(data, f, indent=2)

    return {
        "status": "published",
        "count": len(sorted_items)
    }