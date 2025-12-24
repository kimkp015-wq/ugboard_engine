# data/index.py

import json
from pathlib import Path
from typing import Dict, List

INDEX_FILE = Path("data/index.json")


# -------------------------
# Internal helpers
# -------------------------

def _safe_read():
    try:
        if not INDEX_FILE.exists():
            return None
        return json.loads(INDEX_FILE.read_text())
    except Exception:
        return None


def _safe_write(data) -> None:
    INDEX_FILE.parent.mkdir(parents=True, exist_ok=True)
    tmp = INDEX_FILE.with_suffix(".tmp")
    tmp.write_text(json.dumps(data, indent=2))
    tmp.replace(INDEX_FILE)


# -------------------------
# Public READ API
# -------------------------

def get_index() -> Dict:
    """
    Public read-only index for charts.

    Guarantees:
    - Never crashes
    - Always returns a dict
    """
    data = _safe_read()
    if not isinstance(data, dict):
        return {
            "weeks": [],
            "latest_week": None,
        }
    return data


# -------------------------
# Admin helpers
# -------------------------

def record_week_publish(week_id: str) -> None:
    """
    Record a published chart week.
    """
    index = get_index()

    weeks: List[str] = index.get("weeks", [])
    if week_id not in weeks:
        weeks.append(week_id)

    index["weeks"] = weeks
    index["latest_week"] = week_id

    _safe_write(index)


def week_already_published(week_id: str) -> bool:
    """
    Idempotency guard for weekly publish.
    """
    index = get_index()
    return week_id in index.get("weeks", [])
    def week_already_published(week_id: str) -> bool:
    """
    Guard against double-publishing the same week.
    """
    for entry in _load_index():
        if entry.get("week_id") == week_id:
            return True
    return False