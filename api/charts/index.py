from pathlib import Path
import json
from typing import Dict, List

INDEX_FILE = Path("data/index.json")


def _safe_read() -> List[Dict]:
    if not INDEX_FILE.exists():
        return []
    try:
        return json.loads(INDEX_FILE.read_text())
    except Exception:
        return []


def _safe_write(data: List[Dict]) -> None:
    INDEX_FILE.parent.mkdir(parents=True, exist_ok=True)
    tmp = INDEX_FILE.with_suffix(".tmp")
    tmp.write_text(json.dumps(data, indent=2))
    tmp.replace(INDEX_FILE)


def record_week_publish(week_id: str) -> None:
    """
    Record a published chart week.
    Idempotent-safe (caller guards duplicates).
    """
    index = _safe_read()
    index.append(
        {
            "week_id": week_id,
        }
    )
    _safe_write(index)


def week_already_published(week_id: str) -> bool:
    """
    Check if a chart week has already been published.
    """
    index = _safe_read()
    return any(entry.get("week_id") == week_id for entry in index)


def get_index() -> List[Dict]:
    """
    Public read-only index for charts.
    """
    return _safe_read()