# data/chart_week.py

from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo
import json

EAT = ZoneInfo("Africa/Kampala")
STATE_FILE = Path("data/chart_week_state.json")


def _load_state() -> dict | None:
    if not STATE_FILE.exists():
        return None
    try:
        return json.loads(STATE_FILE.read_text())
    except Exception:
        return None


def _save_state(state: dict):
    STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    STATE_FILE.write_text(json.dumps(state, indent=2))


def open_new_tracking_week() -> dict:
    """
    Opens a new tracking window.
    Safe & idempotent.
    """
    state = {
        "opened_at": datetime.now(EAT).isoformat(),
        "status": "open",
    }
    _save_state(state)
    return state


def close_tracking_week() -> dict:
    """
    Closes current tracking window.
    """
    state = _load_state() or {}
    state["closed_at"] = datetime.now(EAT).isoformat()
    state["status"] = "closed"
    _save_state(state)
    return state


def is_tracking_open() -> bool:
    """
    Guardrail check for weekly publish.
    """
    state = _load_state()
    return bool(state and state.get("status") == "open")


def current_chart_week() -> dict | None:
    """
    Used by alerts & admin status.
    """
    return _load_state()