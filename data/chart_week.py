# data/chart_week.py

import json
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo
from typing import Dict

EAT = ZoneInfo("Africa/Kampala")

STATE_FILE = Path("data/chart_week_state.json")
INDEX_FILE = Path("data/index.json")


# -------------------------
# Helpers
# -------------------------
def _now() -> str:
    return datetime.now(EAT).isoformat()


def _load_json(path: Path) -> Dict:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text())
    except Exception:
        return {}


def _save_json(path: Path, data: Dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2))


def _update_index(update: Dict) -> None:
    """
    Update central index.json safely.
    """
    index = _load_json(INDEX_FILE)
    index.update(update)
    _save_json(INDEX_FILE, index)


# -------------------------
# Chart week logic
# -------------------------
def is_tracking_open() -> bool:
    """
    Returns True if tracking window is open.
    """
    return _load_json(STATE_FILE).get("status") == "open"


def open_new_tracking_week() -> Dict:
    """
    Opens a new tracking window.

    Guarantees:
    - Always creates a new week_id
    - Overwrites safely
    - Idempotent for engine restarts
    - Updates index.json
    """
    week_id = datetime.now(EAT).strftime("%Y-W%U")

    state = {
        "week_id": week_id,
        "status": "open",
        "opened_at": _now(),
    }

    _save_json(STATE_FILE, state)

    _update_index(
        {
            "current_week": week_id,
            "week_status": "open",
            "week_opened_at": state["opened_at"],
        }
    )

    return state


def close_tracking_week() -> Dict:
    """
    Closes the current tracking window.

    Safe even if:
    - No week exists
    - Already closed

    Updates index.json
    """
    state = _load_json(STATE_FILE)

    if state.get("status") == "closed":
        return state

    state.update(
        {
            "status": "closed",
            "closed_at": _now(),
        }
    )

    _save_json(STATE_FILE, state)

    _update_index(
        {
            "week_status": "closed",
            "week_closed_at": state["closed_at"],
        }
    )

    return state


def current_chart_week() -> Dict:
    """
    Read-only accessor for current chart week state.
    """
    return _load_json(STATE_FILE)