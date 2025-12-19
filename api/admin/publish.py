from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
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


class PublishTop100(BaseModel):
    items: List[Top100Item]


@router.post("/publish/top100")
def publish_top100(payload: PublishTop100):
    if not payload.items:
        raise HTTPException(status_code=400, detail="Top100 cannot be empty")

    if len(payload.items) > 100:
        raise HTTPException(status_code=400, detail="Maximum 100 items allowed")

    os.makedirs("data", exist_ok=True)

    data = {
        "updated_at": datetime.utcnow().isoformat(),
        "items": [item.dict() for item in payload.items]
    }

    with open(DATA_FILE, "w") as f:
        json.dump(data, f, indent=2)

    return {
        "status": "published",
        "count": len(payload.items)
    }