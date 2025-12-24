# data/index.py

import json
from pathlib import Path
from datetime import datetime, timezone
from typing import Dict, List, Set, Any

INDEX_FILE = Path("data/index.json")


# -------------------------
# Internal helpers
# -------------------------

def _empty_index() -> Dict[str, Any]:
    return {
        "published_weeks": [],
        "last_published_at": None,
    }


def _load_index() -> Dict[str, Any]:
    """
    Load index.json safely.
    Always returns a valid index dict.
    """
    if not INDEX_FILE.exists():
        return _empty_index()

    try:
        data = json.loads(INDEX_FILE.read_text())

        if not isinstance(data, dict):
            raise ValueError("Index is not a dict")

        if "published_weeks" not in data or not isinstance(data["published_weeks"], list):
            raise ValueError("Invalid published_weeks")

        return {
            "published_weeks": list(map(str, data["published_weeks"])),
            "last_published_at": data.get("last_published_at"),
        }

    except Exception:
        # Never crash engine boot due to index corruption
        return _empty_index()


def _save_index(data: Dict[str, Any]) -> None:
    """
    Atomic write to index.json.
    """
    INDEX_FILE.parent.mkdir(parents=True, exist_ok=True)

    tmp = INDEX_FILE.with_suffix(".tmp")
    tmp.write_text(json.dumps(data, indent=2))
    tmp.replace(INDEX_FILE)


def _now_utc() -> str:
    return datetime.now(timezone.utc).isoformat()


# -------------------------
# Public API
# -------------------------

def get_index() -> Dict[str, Any]:
    """
    Read-only index access.

    Used by:
    - public charts
    - admin dashboards
    - internal verification
    """
    return _load_index()


def record_week_publish(week_id: str) -> None:
    """
    Record a successfully published chart week.
    Idempotent and safe.
    """
    index = _load_index()

    weeks: Set[str] = set(index.get("published_weeks", []))
    weeks.add(week_id)

    index["published_weeks"] = sorted(weeks)
    index["last_published_at"] = _now_utc()

    _save_index(index)


def week_already_published(week_id: str) -> bool:
    """
    Idempotency guard.
    """
    index = _load_index()
    return week_id in index.get("published_weeks", [])