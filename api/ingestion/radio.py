from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field
import json
import os

router = APIRouter()


class RadioIngest(BaseModel):
    title: str
    artist: str
    plays: int = Field(ge=0)


def resolve_radio_path():
    candidates = [
        "api/data/radio.json",
        "data/radio.json",
        "ingestion/radio.json",
        "/app/api/data/radio.json",
        "/app/data/radio.json",
        "/app/ingestion/radio.json",
    ]

    for path in candidates:
        dir_name = os.path.dirname(path)
        if dir_name:
            os.makedirs(dir_name, exist_ok=True)
        return path  # first writable path


@router.post("/radio")
def ingest_radio(payload: RadioIngest):
    path = resolve_radio_path()

    # Load existing data safely
    if os.path.exists(path):
        try:
            with open(path, "r") as f:
                data = json.load(f)
        except Exception:
            data = {"items": []}
    else:
        data = {"items": []}

    if "items" not in data or not isinstance(data["items"], list):
        data["items"] = []

    # Append new signal
    data["items"].append({
        "title": payload.title.strip(),
        "artist": payload.artist.strip(),
        "plays": payload.plays
    })

    # Write safely
    try:
        with open(path, "w") as f:
            json.dump(data, f, indent=2)
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to write radio ingestion file: {str(e)}"
        )

    return {
        "status": "ok",
        "ingested": 1
    }