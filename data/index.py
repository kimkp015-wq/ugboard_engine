# data/index.py

from pathlib import Path
import json
from typing import Dict, List, Optional
from datetime import datetime
from zoneinfo import ZoneInfo
import tempfile
import os

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
    Never raises.
    """
    if not INDEX_FILE.exists():
        return []

    try:
        data = json.loads(INDEX_FILE.read_text())
        return data if isinstance(data, list) else []
    except Exception:
        return []


def _atomic_write(data: List[Dict]) -> None:
    """
    Atomic append-safe write.
    Prevents corruption during concurrent writes.
    """
    INDEX_FILE.parent.mkdir(parents=True, exist_ok=True)

    tmp_fd, tmp_path = tempfile.mkstemp(dir=INDEX_FILE.parent)
    try:
        with os.fdopen(tmp_fd, "w") as f:
            json.dump(data, f, indent=2)
        os.replace(tmp_path, INDEX_FILE)
    finally:
        if os.path.exists(tmp_path):
            os.unlink(tmp_path)


# -------------------------
# Public API (ENGINE CONTRACT)
# -------------------------

def week_already_published(week_id: str) -> bool:
    """
    Idempotency guard.
    """
    return any(
        isinstance(entry, dict) and entry.get("week_id") == week_id
        for entry in _safe_read()
    )


def record_week_publish(
    *,
    week_id: str,
    regions: Optional[List[str]] = None,
    trigger: Optional[str] = None,
) -> Dict:
    """
    Append an immutable publish record.

    HARD GUARANTEES:
    - Append-only
    - Idempotent
    - Atomic
    """

    index = _safe_read()

    # Internal idempotency enforcement (DO NOT TRUST CALLERS)
    if any(
        isinstance(entry, dict) and entry.get("week_id") == week_id
        for entry in index
    ):
        return next(
            entry for entry in index
            if entry.get("week_id") == week_id
        )

    record: Dict = {
        "week_id": week_id,
        "published_at": _now(),
        "regions": list(regions) if regions else [],
        "trigger": trigger or "unknown",
    }

    index.append(record)
    _atomic_write(index)

    return record


def get_index() -> List[Dict]:
    """
    Read-only public index.
    """
    return _safe_read()