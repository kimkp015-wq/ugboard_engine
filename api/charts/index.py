# data/index.py

from pathlib import Path
import json
from typing import Dict, List
from datetime import datetime
from zoneinfo import ZoneInfo

INDEX_FILE = Path("data/index.json")
EAT = ZoneInfo("Africa/Kampala")


# -------------------------
# Internal helpers
# -------------------------

def _now() -> str:
    return datetime.now(EAT).isoformat()


def _safe_read() -> List[Dict]:
    if not INDEX_FILE.exists():
        return []

    try:
        data = json.loads(INDEX_FILE.read_text())
        return data if isinstance(data, list) else []
    except Exception:
        return []


def _safe_write(data: List[Dict]) -> None:
    INDEX_FILE.parent.mkdir(parents=True, exist_ok=True)
    tmp = INDEX_FILE.with_suffix(".tmp")
    tmp.write_text(json.dumps(data, indent=2))
    tmp.replace(INDEX_FILE)


# -------------------------
# Public API
# -------------------------

def record_week_publish(
    *,
    week_id: str,
    regions: List[str] | None = None,
    trigger: str | None = None,
) -> Dict:
    """
    Append an immutable publish record.
    Safe to call once per week.
    """

    index = _safe_read()

    record = {
        "week_id": week_id,
        "published_at": _now(),
    }

    if regions:
        record["regions"] = regions

    if trigger:
        record["trigger"] = trigger

    index.append(record)
    _safe_write(index)

    return record


def week_already_published(week_id: str) -> bool:
    """
    Idempotency guard.
    """
    return any(
        entry.get("week_id") == week_id
        for entry in _safe_read()
    )


def get_index() -> List[Dict]:
    """
    Read-only public index.
    """
    return _safe_read()