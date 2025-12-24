# data/chart_week.py

import json
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo
from typing import Dict

EAT = ZoneInfo("Africa/Kampala")
STATE_FILE = Path("data/chart_week_state.json")


# =========================
# Internal helpers
# =========================

def _now() -> str:
    return datetime.now(EAT).isoformat()


def _load_state() -> Dict:
    """
    Load week state safely. Never raises.
    """
    if not STATE_FILE.exists():
        return {}

    try:
        data = json.loads(STATE_FILE.read_text())
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def _save_state(state: Dict) -> None:
    STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    STATE_FILE.write_text(json.dumps(state, indent=2))


# =========================
# Public API (engine contracts)
# =========================

def get_current_week_id() -> str:
    """
    Canonical week ID generator.
    Always returns a string like "2025-W52".
    """
    state = _load_state()
    week_id = state.get("week_id")
    if isinstance(week_id, str) and week_id:
        return week_id

    # If no week stored, generate from current date
    now = datetime.now(EAT)
    year, week, _ = now.isocalendar()
    return f"{year}-W{week:02d}"


def current_chart_week() -> Dict:
    """
    Returns the full stored week state if present,
    or a minimal object if not yet initialized.
    """
    state = _load_state()
    if not isinstance(state, dict):
        return {}
    return state


def close_tracking_week() -> Dict:
    """
    Mark the current week as closed.
    Idempotent: if already closed, does nothing.
    """
    state = _load_state()

    if state.get("status") == "closed":
        return state

    state["status"] = "closed"
    state["closed_at"] = _now()

    _save_state(state)
    return state


def open_new_tracking_week() -> Dict:
    """
    Create a new tracking week and return its state.
    Overwrites previous state safely.
    """
    state = {
        "week_id": get_current_week_id(),
        "status": "open",
        "opened_at": _now(),
    }

    _save_state(state)
    return state