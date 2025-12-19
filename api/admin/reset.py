from fastapi import APIRouter
import os
import json

router = APIRouter(prefix="/admin", tags=["admin"])

DATA_FILES = [
    "data/boost.json",
    "data/radio.json",
    "data/youtube.json",
    "data/tv.json",
    "data/top100.json"
]


@router.post("/reset")
def reset_engine():
    cleared = []

    for file in DATA_FILES:
        if os.path.exists(file):
            with open(file, "w") as f:
                json.dump({}, f)
            cleared.append(file)

    return {
        "status": "reset complete",
        "files_cleared": cleared
    }