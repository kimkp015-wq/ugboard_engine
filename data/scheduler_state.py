# data/scheduler_state.py

from pathlib import Path
from datetime import datetime
from zoneinfo import ZoneInfo

EAT = ZoneInfo("Africa/Kampala")
FILE = Path("data/last_scheduler_run.txt")


def record_scheduler_run() -> None:
    """
    Record the last successful scheduler run timestamp.
    Safe to call multiple times.
    """
    FILE.parent.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.now(EAT).isoformat()
    FILE.write_text(timestamp)


def get_last_scheduler_run() -> str | None:
    """
    Return last scheduler run timestamp (ISO string).
    Returns None if never run or file unreadable.
    """
    if not FILE.exists():
        return None

    try:
        value = FILE.read_text().strip()
        return value or None
    except Exception:
        return None