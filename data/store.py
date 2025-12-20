import json
import os

DATA_PATHS = [
    "api/data/top100.json",
    "data/top100.json",
    "ingestion/top100.json",
    "/app/api/data/top100.json",
    "/app/data/top100.json",
    "/app/ingestion/top100.json",
]


def resolve_data_path():
    for path in DATA_PATHS:
        if os.path.exists(path):
            return path
    return None


def load_items():
    path = resolve_data_path()
    if not path:
        return []

    try:
        with open(path, "r") as f:
            data = json.load(f)
        return data.get("items", [])
    except Exception:
        return []


def save_items(items):
    path = resolve_data_path()

    if not path:
        # create default location
        path = "data/top100.json"
        os.makedirs("data", exist_ok=True)

    try:
        with open(path, "w") as f:
            json.dump({"items": items}, f, indent=2)
    except Exception:
        pass