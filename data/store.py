# data/store.py (excerpt)

import json
from pathlib import Path
from typing import List, Dict

STORE_FILE = Path("data/items.json")


def load_items() -> List[Dict]:
    if not STORE_FILE.exists():
        return []

    try:
        data = json.loads(STORE_FILE.read_text())
        return data if isinstance(data, list) else []
    except Exception:
        return []


def _atomic_write(data: List[Dict]) -> None:
    STORE_FILE.parent.mkdir(parents=True, exist_ok=True)
    tmp = STORE_FILE.with_suffix(".tmp")
    tmp.write_text(json.dumps(data, indent=2))
    tmp.replace(STORE_FILE)


def upsert_items(items: List[Dict]) -> int:
    """
    Insert new items only.
    Hard dedup by (source, video_id).

    Returns number of inserted items.
    """
    if not items:
        return 0

    existing = load_items()

    # Build lookup: (source, video_id)
    seen = {
        (i.get("source"), i.get("video_id"))
        for i in existing
        if isinstance(i, dict)
    }

    inserted = 0

    for item in items:
        if not isinstance(item, dict):
            continue

        key = (item.get("source"), item.get("video_id"))

        # Reject invalid or duplicate
        if None in key or key in seen:
            continue

        existing.append(item)
        seen.add(key)
        inserted += 1

    if inserted:
        _atomic_write(existing)

    return inserted
    # ---------------------------------
# Compatibility alias (DO NOT REMOVE)
# ---------------------------------

def upsert_item(*args, **kwargs):
    """
    Backward-compatible alias.
    Radio/TV ingestion depends on this name.
    """
    # choose the canonical upsert function you already use
    return upsert_record(*args, **kwargs)
