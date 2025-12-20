import json
import os

DATA_FILE = "ingestion/top100.json"


def load_items():
    if not os.path.exists(DATA_FILE):
        return []

    with open(DATA_FILE, "r") as f:
        data = json.load(f)

    return data.get("items", [])


def save_items(items):
    os.makedirs("ingestion", exist_ok=True)

    with open(DATA_FILE, "w") as f:
        json.dump({"items": items}, f, indent=2)