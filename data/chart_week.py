# data/chart_week.py

import json
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo
from typing import Dict

EAT = ZoneInfo("Africa/Kampala")
STATE_FILE = Path("data/chart_week_state.json")


def _now() -> str:
    return datetime.now(EAT).isoformat()


def _load_state() -> Dict:
    """
    Load chart week state from disk.
    Safe: never raises.
    """
    if not STATE_FILE.exists():
        return {}

    try:
        return json.loads(STATE_FILE.read_text())
    except Exception:
        # Corrupt file fallback
        return {}


def is_tracking_open() -> bool:
    """
    Returns True if tracking window is open.
    """
    return _load_state().get("status") == "open"


def open_new_tracking_week() -> Dict:
    """
    Opens a new tracking window.

    Guarantees:
    - Always creates a new week_id
    - Overwrites safely
    - Idempotent for engine restarts
    """
    STATE_FILE.parent.mkdir(parents=True, exist_ok=True)

    state = {
        "week_id": datetime.now(EAT).strftime("%Y-W%U"),
        "status": "open",
        "opened_at": _now(),
    }

    STATE_FILE.write_text(json.dumps(state, indent=2))
    return state


def close_tracking_week() -> Dict:
    """
    Closes the current tracking window.

    Safe even if:
    - No week exists
    - Already closed
    """
    STATE_FILE.parent.mkdir(parents=True, exist_ok=True)

    state = _load_state()

    if state.get("status") == "closed":
        return state

    state.update(
        {
            "status": "closed",
            "closed_at": _now(),
        }
    )

    STATE_FILE.write_text(json.dumps(state, indent=2))
    return state


def current_chart_week() -> Dict:
    """
    Read-only accessor for current chart week state.
    """
    return _load_state()