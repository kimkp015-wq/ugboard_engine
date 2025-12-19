from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
import json
import os

router = APIRouter()

DATA_FILE = "data/top100.json"


class BoostRequest(BaseModel):
    position: int
    points: int


@router.get("/charts/top100")
def get_top100():
    if not os.path.exists(DATA_FILE):
        raise HTTPException(status_code=404, detail="Top 100 not published")

    with open(DATA_FILE, "r") as f:
        data = json.load(f)

    items = data.get("items", [])

    boosted = data.get("boosts", {})

    scored_items = []
    for item in items:
        position = item["position"]
        base_score = 101 - position
        boost = boosted.get(str(position), 0)

        scored_items.append({
            **item,
            "base_score": base_score,
            "boost": boost,
            "score": base_score + boost
        })

    return {
        "status": "ok",
        "count": len(scored_items),
        "items": scored_items
    }


@router.post("/charts/top100/boost")
def boost_song(payload: BoostRequest):
    if not os.path.exists(DATA_FILE):
        raise HTTPException(status_code=404, detail="Top 100 not published")

    if payload.points <= 0:
        raise HTTPException(status_code=400, detail="Boost points must be positive")

    with open(DATA_FILE, "r") as f:
        data = json.load(f)

    boosts = data.get("boosts", {})
    boosts[str(payload.position)] = boosts.get(str(payload.position), 0) + payload.points

    data["boosts"] = boosts

    with open(DATA_FILE, "w") as f:
        json.dump(data, f, indent=2)

    return {
        "status": "boosted",
        "position": payload.position,
        "added_points": payload.points,
        "total_boost": boosts[str(payload.position)]
    }