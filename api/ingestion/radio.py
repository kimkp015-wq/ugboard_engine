from fastapi import APIRouter
from pydantic import BaseModel, field_validator
from datetime import datetime
import json
import os

router = APIRouter()

RADIO_FILE = "data/radio.json"
BOOST_FILE = "data/boost.json"

RADIO_WEIGHT = 5  # radio is very powerful


class RadioPlay(BaseModel):
    title: str
    artist: str
    plays: int

    @field_validator("plays")
    @classmethod
    def positive(cls, v):
        if v <= 0:
            raise ValueError("plays must be positive")
        return v


def load_json(path):
    if not os.path.exists(path):
        return []
    with open(path, "r") as f:
        return json.load(f)


def save_json(path, data):
    os.makedirs("data", exist_ok=True)
    with open(path, "w") as f:
        json.dump(data, f, indent=2)


@router.post("/ingest/radio")
def ingest_radio(payload: RadioPlay):
    radio = load_json(RADIO_FILE)
    boosts = load_json(BOOST_FILE)

    # save radio play
    radio.append({
        "title": payload.title,
        "artist": payload.artist,
        "plays": payload.plays,
        "time": datetime.utcnow().isoformat()
    })

    # convert radio plays → boost points
    boosts.append({
        "title": payload.title,
        "artist": payload.artist,
        "points": payload.plays * RADIO_WEIGHT,
        "source": "radio",
        "time": datetime.utcnow().isoformat()
    })

    save_json(RADIO_FILE, radio)
    save_json(BOOST_FILE, boosts)

    return {
        "status": "radio ingested",
        "title": payload.title,
        "artist": payload.artist,
        "points_added": payload.plays * RADIO_WEIGHT
    }