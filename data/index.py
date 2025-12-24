# data/index.py

import json
from pathlib import Path
from datetime import datetime
from typing import Set, Dict

INDEX_FILE = Path("data/index.json")


# -------------------------
# Internal helpers
# -------------------------

def _load_index() -> Dict:
    if not INDEX_FILE.exists():
        return {
            "published_weeks": [],
            "last_published_at": None,
        }

    try:
        data = json.loads(INDEX_FILE.read_text())
        if not isinstance(data, dict):
            raise ValueError("Invalid index format")
        return data
    except Exception:
        return {
            "published_weeks": [],
            "last_published_at": None,
        }


def _save_index(data: Dict) -> None:
    INDEX_FILE.parent.mkdir(parents=True, exist_ok=True)
    tmp = INDEX_FILE.with_suffix(".tmp")
    tmp.write_text(json.dumps(data, indent=2))
    tmp.replace(INDEX_FILE)


# -------------------------
# Public API
# -------------------------

def get_index() -> Dict:
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
    Idempotent.
    """
    index = _load_index()
    weeks: Set[str] = set(index.get("published_weeks", []))

    weeks.add(week_id)

    index["published_weeks"] = sorted(weeks)
    index["last_published_at"] = datetime.utcnow().isoformat()

    _save_index(index)


def week_already_published(week_id: str) -> bool:
    """
    Check if a chart week has already been published.
    Used as idempotency guard.
    """
    index = _load_index()
    return week_id in index.get("published_weeks", [])