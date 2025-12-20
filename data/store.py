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


def load_top100():
    path = resolve_data_path()
    if not path:
        return []

    try:
        with open(path, "r") as f:
            data = json.load(f)
        return data.get("items", [])
    except Exception:
        return []


def save_top100(items):
    path = resolve_data_path()

    if not path:
        path = "data/top100.json"
        os.makedirs("data", exist_ok=True)

    try:
        with open(path, "w") as f:
            json.dump({"items": items}, f, indent=2)
    except Exception:
        pass


# --- BACKWARD COMPATIBILITY (VERY IMPORTANT) ---
# These aliases prevent future import crashes

def load_items():
    return load_top100()


def save_items(items):
    save_top100(items)