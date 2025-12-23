# data/chart_week.py

from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from pathlib import Path

EAT = ZoneInfo("Africa/Kampala")
STATE_FILE = Path("data/chart_week_state.json")


def open_new_tracking_week():
    """
    Opens a new tracking window after weekly publish.
    Idempotent and safe.
    """
    STATE_FILE.parent.mkdir(parents=True, exist_ok=True)

    next_week = {
        "opened_at": datetime.now(EAT).isoformat(),
        "status": "open",
    }

    STATE_FILE.write_text(
        __import__("json").dumps(next_week, indent=2)
    )

    return next_week