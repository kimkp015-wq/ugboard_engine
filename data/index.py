# data/index.py

import json
from pathlib import Path
from datetime import datetime
from typing import Set

INDEX_FILE = Path("data/index.json")


# -------------------------
# Internal helpers
# -------------------------

def _load_index() -> dict:
    if not INDEX_FILE.exists():
        return {"published_weeks": []}

    try:
        return json.loads(INDEX_FILE.read_text())
    except Exception:
        return {"published_weeks": []}


def _save_index(data: dict) -> None:
    INDEX_FILE.parent.mkdir(parents=True, exist_ok=True)
    tmp = INDEX_FILE.with_suffix(".tmp")
    tmp.write_text(json.dumps(data, indent=2))
    tmp.replace(INDEX_FILE)


# -------------------------
# Public API
# -------------------------

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