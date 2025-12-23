# data/chart_week.py

from datetime import datetime
from zoneinfo import ZoneInfo
from pathlib import Path
import json

EAT = ZoneInfo("Africa/Kampala")
STATE_FILE = Path("data/chart_week_state.json")


def is_tracking_open() -> bool:
    """
    Returns True if a tracking window is currently open.
    Safe if state file does not exist or is corrupted.
    """
    if not STATE_FILE.exists():
        return False

    try:
        data = json.loads(STATE_FILE.read_text())
        return data.get("status") == "open"
    except Exception:
        # Fail-safe: assume closed to avoid blocking system
        return False


def open_new_tracking_week() -> dict:
    """
    Opens a new tracking window after weekly publish.
    Idempotent and safe.
    """
    STATE_FILE.parent.mkdir(parents=True, exist_ok=True)

    state = {
        "opened_at": datetime.now(EAT).isoformat(),
        "status": "open",
    }

    STATE_FILE.write_text(json.dumps(state, indent=2))
    return state