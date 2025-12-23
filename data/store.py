# data/store.py

import json
from pathlib import Path
from typing import List, Dict

ITEMS_FILE = Path("data/items.json")


def _safe_read_json(path: Path):
    try:
        return json.loads(path.read_text())
    except Exception:
        return None


def load_items() -> List[Dict]:
    """
    Load chart items safely.
    Never raises.
    Returns empty list on failure.
    """
    if not ITEMS_FILE.exists():
        return []

    data = _safe_read_json(ITEMS_FILE)

    if not isinstance(data, list):
        return []

    # Defensive filtering
    cleaned = []
    for item in data:
        if not isinstance(item, dict):
            continue
        cleaned.append(item)

    return cleaned


def save_items(items: List[Dict]) -> None:
    """
    Persist items atomically.
    """
    if not isinstance(items, list):
        raise ValueError("items must be a list")

    ITEMS_FILE.parent.mkdir(parents=True, exist_ok=True)

    tmp = ITEMS_FILE.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(items, indent=2))
    tmp.replace(ITEMS_FILE)