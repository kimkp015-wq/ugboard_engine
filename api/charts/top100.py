import json
import os
from fastapi import APIRouter
from api.scoring.scorer import calculate_score

router = APIRouter()

DATA_FILE = "data/top100.json"


def generate_top100():
    songs = []

    for i in range(1, 101):
        score = calculate_score(
            song_title=f"Song {i}",
            artist="Ugandan Artist",
            songboost_rank=i,
            previous_rank=i + 3,
            boost=0
        )

        songs.append({
            "rank": i,
            "title": f"Song {i}",
            "artist": "Ugandan Artist",
            "score": score
        })

    return {
        "chart": "UG Top 100",
        "total": len(songs),
        "songs": songs
    }


@router.get("/top100")
def get_top_100():
    # If data already exists → use it
    if os.path.exists(DATA_FILE):
        with open(DATA_FILE, "r") as f:
            return json.load(f)

    # Otherwise → generate and save
    data = generate_top100()

    os.makedirs("data", exist_ok=True)
    with open(DATA_FILE, "w") as f:
        json.dump(data, f, indent=2)

    return data