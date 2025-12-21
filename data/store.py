# data/store.py

import json
from pathlib import Path

DATA_FILE = Path("data/items.json")

def load_items():
    if not DATA_FILE.exists():
        return []
    return json.loads(DATA_FILE.read_text())

def save_items(items):
    DATA_FILE.parent.mkdir(parents=True, exist_ok=True)
    DATA_FILE.write_text(json.dumps(items, indent=2))