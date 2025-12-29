# data/store.py

import json
from pathlib import Path
from typing import Dict, List
from datetime import datetime
from zoneinfo import ZoneInfo

EAT = ZoneInfo("Africa/Kampala")

STORE_FILE = Path("data/items.json")


# -------------------------
# Helpers
# -------------------------

def _now() -> str:
    return datetime.now(EAT).isoformat()


def _safe_read() -> List[Dict]:
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


# -------------------------
# Public API (ENGINE CONTRACT)
# -------------------------

def load_items() -> List[Dict]:
    """
    Read-only load for charts.
    """
    return _safe_read()


def upsert_item(item: Dict) -> Dict:
    """
    Idempotent insert/update.

    Uniqueness key:
    - source
    - external_id

    Behavior:
    - If exists → update signals + updated_at
    - If new → insert with created_at
    """
    if not isinstance(item, dict):
        raise ValueError("Invalid item payload")

    source = item.get("source")
    external_id = item.get("external_id")

    if not source or not external_id:
        raise ValueError("Item must have source and external_id")

    items = _safe_read()

    for existing in items:
        if (
            existing.get("source") == source
            and existing.get("external_id") == external_id
        ):
            # 🔁 UPDATE (idempotent)
            existing.update(item)
            existing["updated_at"] = _now()
            _atomic_write(items)
            return existing

    # ➕ INSERT (new)
    record = {
        **item,
        "created_at": _now(),
        "updated_at": _now(),
    }

    items.append(record)
    _atomic_write(items)
    return record
