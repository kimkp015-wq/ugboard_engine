# data/chart_week.py

import json
from datetime import datetime, timedelta
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
    Load chart week state safely.
    Never raises, never crashes boot.
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
    tmp = STATE_FILE.with_suffix(".tmp")
    tmp.write_text(json.dumps(state, indent=2))
    tmp.replace(STATE_FILE)


def _compute_week_id(dt: datetime) -> str:
    year, week, _ = dt.isocalendar()
    return f"{year}-W{week:02d}"


# =========================
# Public API (ENGINE CONTRACT)
# =========================

def get_current_week_id() -> str:
    """
    Return the currently active chart week ID.
    Falls back to calendar week if state is missing.
    """
    state = _load_state()
    week_id = state.get("week_id")

    if isinstance(week_id, str) and week_id:
        return week_id

    return _compute_week_id(datetime.now(EAT))


def current_chart_week() -> Dict:
    """
    Return the full current chart week state.
    Safe for reads by snapshots, charts, admin.
    """
    state = _load_state()
    return state if isinstance(state, dict) else {}


def close_tracking_week() -> Dict:
    """
    Close the current tracking week.
    Idempotent: safe to call multiple times.
    """
    state = _load_state()

    if state.get("status") == "closed":
        return state

    if not state:
        week_id = get_current_week_id()
        state = {
            "week_id": week_id,
            "status": "closed",
            "opened_at": None,
            "closed_at": _now(),
        }
    else:
        state["status"] = "closed"
        state["closed_at"] = _now()

    _save_state(state)
    return state


def open_new_tracking_week() -> Dict:
    """
    Open a brand-new tracking week.
    Advances calendar week safely.
    """
    now = datetime.now(EAT)
    next_week_dt = now + timedelta(days=7)
    week_id = _compute_week_id(next_week_dt)

    state = {
        "week_id": week_id,
        "status": "open",
        "opened_at": _now(),
        "closed_at": None,
    }

    _save_state(state)
    return state


# =========================
# Explicit exports (prevents ImportError)
# =========================

__all__ = [
    "get_current_week_id",
    "current_chart_week",
    "close_tracking_week",
    "open_new_tracking_week",
]