# data/index.py

from pathlib import Path
import json
from typing import Dict, List, Optional
from datetime import datetime
from zoneinfo import ZoneInfo

INDEX_FILE = Path("data/index.json")
EAT = ZoneInfo("Africa/Kampala")


# -------------------------
# Internal helpers
# -------------------------

def _now() -> str:
    """Current timestamp in EAT (ISO-8601)."""
    return datetime.now(EAT).isoformat()


def _safe_read() -> List[Dict]:
    """
    Read index file safely.
    Always returns a list.
    """
    if not INDEX_FILE.exists():
        return []

    try:
        data = json.loads(INDEX_FILE.read_text())
        return data if isinstance(data, list) else []
    except Exception:
        return []


def _safe_write(data: List[Dict]) -> None:
    """
    Atomic write to index.json.
    """
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
    regions: Optional[List[str]] = None,
    trigger: Optional[str] = None,
) -> Dict:
    """
    Append an immutable publish record.

    Safe to call once per week.
    Idempotency must be guarded by caller.
    """

    index = _safe_read()

    record: Dict = {
        "week_id": week_id,
        "published_at": _now(),
    }

    if regions:
        record["regions"] = list(regions)

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
        isinstance(entry, dict) and entry.get("week_id") == week_id
        for entry in _safe_read()
    )


def get_index() -> List[Dict]:
    """
    Read-only public index.
    """
    return _safe_read()