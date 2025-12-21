import json
from pathlib import Path

# -----------------------------
# Paths
# -----------------------------
ITEMS_FILE = Path("data/items.json")
TOP100_FILE = Path("data/top100.json")


# -----------------------------
# Items (raw data)
# -----------------------------
def load_items():
    if not ITEMS_FILE.exists():
        return []
    try:
        return json.loads(ITEMS_FILE.read_text())
    except Exception:
        return []


def save_items(items: list):
    ITEMS_FILE.parent.mkdir(parents=True, exist_ok=True)
    ITEMS_FILE.write_text(json.dumps(items, indent=2))


# -----------------------------
# Top100 (published chart)
# -----------------------------
def load_top100():
    if not TOP100_FILE.exists():
        return {"items": []}
    try:
        return json.loads(TOP100_FILE.read_text())
    except Exception:
        return {"items": []}


def save_top100(data: dict):
    TOP100_FILE.parent.mkdir(parents=True, exist_ok=True)
    TOP100_FILE.write_text(json.dumps(data, indent=2))