import json
import os

TOP100_PATHS = [
    "api/data/top100.json",
    "data/top100.json",
    "/app/api/data/top100.json",
    "/app/data/top100.json",
]


def resolve_top100_path():
    for path in TOP100_PATHS:
        if os.path.exists(path):
            return path
    return None


def load_top100():
    path = resolve_top100_path()
    if not path:
        return {"items": []}

    try:
        with open(path, "r") as f:
            return json.load(f)
    except Exception:
        return {"items": []}


def save_top100(data: dict):
    path = resolve_top100_path()

    # If file doesn't exist yet, create it in data/
    if not path:
        path = "data/top100.json"
        os.makedirs("data", exist_ok=True)

    with open(path, "w") as f:
        json.dump(data, f, indent=2)