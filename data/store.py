# data/store.py

import json
from pathlib import Path

ITEMS_FILE = Path("data/items.json")
REGION_LOCKS_FILE = Path("data/region_locks.json")


# ------------------------
# ITEMS
# ------------------------
def load_items():
    if not ITEMS_FILE.exists():
        return []
    return json.loads(ITEMS_FILE.read_text())


def save_items(items):
    ITEMS_FILE.parent.mkdir(parents=True, exist_ok=True)
    ITEMS_FILE.write_text(json.dumps(items, indent=2))


# ------------------------
# REGION LOCKS
# ------------------------
def load_region_locks():
    if not REGION_LOCKS_FILE.exists():
        return {
            "eastern": False,
            "northern": False,
            "western": False,
        }
    return json.loads(REGION_LOCKS_FILE.read_text())


def save_region_locks(locks: dict):
    REGION_LOCKS_FILE.parent.mkdir(parents=True, exist_ok=True)
    REGION_LOCKS_FILE.write_text(json.dumps(locks, indent=2))