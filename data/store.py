import json
from pathlib import Path
from typing import List, Dict

# Base data directory
DATA_DIR = Path("data")

# Main items storage (used by ingestion & trending)
ITEMS_FILE = DATA_DIR / "items.json"


def load_items() -> List[Dict]:
    """
    Load all tracked songs/items.
    Safe: returns empty list if file does not exist or is invalid.
    """
    try:
        if not ITEMS_FILE.exists():
            return []

        raw = ITEMS_FILE.read_text()
        data = json.loads(raw)

        if not isinstance(data, list):
            return []

        return data

    except Exception:
        # Never crash the engine because of bad data
        return []


def save_items(items: List[Dict]) -> None:
    """
    Save items safely to disk.
    """
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    ITEMS_FILE.write_text(
        json.dumps(items, indent=2)
    )