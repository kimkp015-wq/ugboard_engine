# data/store.py

import json
from pathlib import Path

ITEMS_FILE = Path("data/items.json")
TOP100_FILE = Path("data/top100.json")


def load_items():
    if not ITEMS_FILE.exists():
        return []
    return json.loads(ITEMS_FILE.read_text())


def save_items(items):
    ITEMS_FILE.parent.mkdir(parents=True, exist_ok=True)
    ITEMS_FILE.write_text(json.dumps(items, indent=2))


def load_top100():
    if not TOP100_FILE.exists():
        return {"items": []}
    return json.loads(TOP100_FILE.read_text())


def save_top100(data):
    TOP100_FILE.parent.mkdir(parents=True, exist_ok=True)
    TOP100_FILE.write_text(json.dumps(data, indent=2))