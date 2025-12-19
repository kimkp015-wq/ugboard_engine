import json
import os
from fastapi import APIRouter

router = APIRouter()

DATA_FILE = "data/top100.json"


def calculate_score(rank):
    # simple, stable scoring for now
    return 1000 - (rank * 5)


def generate_top100():
    songs = []

    for i in range(1, 101):
        songs.append({
            "rank": i,
            "title": f"Song {i}",
            "artist": "Ugandan Artist",
            "score": calculate_score(i)
        })

    return {
        "status": "ok",
        "chart": "UG Top 100",
        "total": len(songs),
        "data": songs
    }


@router.get("/top100")
def get_top_100():
    # return cached chart if exists
    if os.path.exists(DATA_FILE):
        with open(DATA_FILE, "r") as f:
            return json.load(f)

    data = generate_top100()

    os.makedirs("data", exist_ok=True)
    with open(DATA_FILE, "w") as f:
        json.dump(data, f, indent=2)

    return data