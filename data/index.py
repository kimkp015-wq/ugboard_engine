# data/index.py

import json
from pathlib import Path
from typing import Dict

INDEX_FILE = Path("data/week_index.json")


# -------------------------
# Internal helpers
# -------------------------

def _safe_read() -> Dict:
    if not INDEX_FILE.exists():
        return {}

    try:
        return json.loads(INDEX_FILE.read_text())
    except Exception:
        return {}


def _safe_write(data: Dict) -> None:
    INDEX_FILE.parent.mkdir(parents=True, exist_ok=True)
    tmp = INDEX_FILE.with_suffix(".tmp")
    tmp.write_text(json.dumps(data, indent=2))
    tmp.replace(INDEX_FILE)


# -------------------------
# Public API
# -------------------------

def week_already_published(week_id: str) -> bool:
    """
    Returns True if this week was already published.
    Never raises.
    """
    index = _safe_read()
    return index.get(week_id) is True


def record_week_publish(week_id: str) -> None:
    """
    Records that a chart week has been published.
    Idempotent and safe.
    """
    index = _safe_read()
    index[week_id] = True
    _safe_write(index)