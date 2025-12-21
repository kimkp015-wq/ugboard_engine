# data/store.py

import json
from pathlib import Path

ITEMS_FILE = Path("data/items.json")


def load_items():
    if not ITEMS_FILE.exists():
        return []
    return json.loads(ITEMS_FILE.read_text())


def save_items(items):
    ITEMS_FILE.parent.mkdir(parents=True, exist_ok=True)
    ITEMS_FILE.write_text(json.dumps(items, indent=2))