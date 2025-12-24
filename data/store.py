# data/store.py

import json
from pathlib import Path
from typing import List, Dict, Optional

ITEMS_FILE = Path("data/items.json")


# ------------------------
# Internal helpers
# ------------------------

def _safe_read_json(path: Path):
    try:
        return json.loads(path.read_text())
    except Exception:
        return None


def _safe_write_json(path: Path, data) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(data, indent=2))
    tmp.replace(path)


def _calculate_score(item: Dict) -> int:
    """
    Deterministic score calculator (v1).

    score =
        youtube_views * 1
      + radio_plays   * 50
      + tv_appearances * 80
    """
    return (
        int(item.get("youtube_views", 0)) * 1
        + int(item.get("radio_plays", 0)) * 50
        + int(item.get("tv_appearances", 0)) * 80
    )


# ------------------------
# Public API
# ------------------------

def load_items() -> List[Dict]:
    """
    Load chart items safely.

    Guarantees:
    - Never raises
    - Always returns a list
    - Filters invalid records
    """
    if not ITEMS_FILE.exists():
        return []

    data = _safe_read_json(ITEMS_FILE)

    if not isinstance(data, list):
        return []

    cleaned: List[Dict] = []

    for item in data:
        if not isinstance(item, dict):
            continue

        if "song_id" not in item:
            continue

        cleaned.append(item)

    return cleaned


def get_item_by_song_id(song_id: str) -> Optional[Dict]:
    """
    Fetch a single item by song_id.
    """
    for item in load_items():
        if item.get("song_id") == song_id:
            return item
    return None


def upsert_item(new_item: Dict) -> Dict:
    """
    Insert or update a song record using song_id as primary key.

    Guarantees:
    - Idempotent
    - Safe for repeated ingestion
    - Score always recalculated
    """
    if not isinstance(new_item, dict):
        raise ValueError("item must be a dict")

    song_id = new_item.get("song_id")
    if not song_id or not isinstance(song_id, str):
        raise ValueError("Missing required field: song_id")

    items = load_items()
    updated = False

    for idx, item in enumerate(items):
        if item.get("song_id") == song_id:
            merged = {**item, **new_item}
            merged["score"] = _calculate_score(merged)
            items[idx] = merged
            updated = True
            break

    if not updated:
        new_item["score"] = _calculate_score(new_item)
        items.append(new_item)

    _safe_write_json(ITEMS_FILE, items)
    return new_item


def delete_item(song_id: str) -> bool:
    """
    Delete a song by song_id.
    """
    items = load_items()
    filtered = [i for i in items if i.get("song_id") != song_id]

    if len(filtered) == len(items):
        return False

    _safe_write_json(ITEMS_FILE, filtered)
    return True