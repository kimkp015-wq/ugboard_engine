import json
import os

DATA_PATHS = [
    "data/top100.json",
    "api/data/top100.json",
    "ingestion/top100.json",
    "/app/data/top100.json",
    "/app/api/data/top100.json",
    "/app/ingestion/top100.json",
]


def resolve_data_path():
    for path in DATA_PATHS:
        directory = os.path.dirname(path)
        if directory and not os.path.exists(directory):
            try:
                os.makedirs(directory, exist_ok=True)
            except Exception:
                pass

        if os.path.exists(path):
            return path

    # fallback → create default
    fallback = DATA_PATHS[0]
    os.makedirs(os.path.dirname(fallback), exist_ok=True)
    save_items([])
    return fallback


def load_items():
    path = resolve_data_path()
    try:
        with open(path, "r") as f:
            data = json.load(f)
            return data.get("items", [])
    except Exception:
        return []


def save_items(items):
    path = resolve_data_path()
    try:
        with open(path, "w") as f:
            json.dump({"items": items}, f, indent=2)
    except Exception:
        pass