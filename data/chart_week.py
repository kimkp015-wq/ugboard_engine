# data/chart_week.py

from pathlib import Path
import json
from datetime import datetime
from zoneinfo import ZoneInfo

STATE_FILE = Path("data/chart_week.json")
EAT = ZoneInfo("Africa/Kampala")


# -------------------------
# Internal helpers
# -------------------------

def _now() -> str:
    return datetime.now(EAT).isoformat()


def _load_state() -> dict:
    if not STATE_FILE.exists():
        return {
            "current_week_id": "week_1",
            "opened_at": _now(),
            "closed_at": None,
        }

    try:
        data = json.loads(STATE_FILE.read_text())
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def _save_state(state: dict) -> None:
    STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    tmp = STATE_FILE.with_suffix(".tmp")
    tmp.write_text(json.dumps(state, indent=2))
    tmp.replace(STATE_FILE)


# -------------------------
# Public API
# -------------------------

def get_current_week_id() -> str:
    """
    Return the active tracking week ID.
    Safe default if state is missing.
    """
    state = _load_state()
    return state.get("current_week_id", "week_1")


def close_tracking_week() -> None:
    """
    Close the currently active tracking week.
    Idempotent.
    """
    state = _load_state()

    if state.get("closed_at"):
        return

    state["closed_at"] = _now()
    _save_state(state)


def open_new_tracking_week() -> str:
    """
    Open the next tracking week.
    """
    state = _load_state()
    current = state.get("current_week_id", "week_1")

    try:
        number = int(current.split("_")[-1]) + 1
    except Exception:
        number = 1

    new_week = f"week_{number}"

    new_state = {
        "current_week_id": new_week,
        "opened_at": _now(),
        "closed_at": None,
    }

    _save_state(new_state)
    return new_week