# data/chart_week.py

import json
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

EAT = ZoneInfo("Africa/Kampala")
STATE_FILE = Path("data/chart_week_state.json")


def _load_state() -> dict:
    if not STATE_FILE.exists():
        return {}

    try:
        return json.loads(STATE_FILE.read_text())
    except json.JSONDecodeError:
        return {}
    except Exception:
        return {}


def is_tracking_open() -> bool:
    state = _load_state()
    return state.get("status") == "open"


def open_new_tracking_week() -> dict:
    STATE_FILE.parent.mkdir(parents=True, exist_ok=True)

    state = {
        "opened_at": datetime.now(EAT).isoformat(),
        "status": "open",
    }

    STATE_FILE.write_text(json.dumps(state, indent=2))
    return state


def close_tracking_week() -> dict:
    state = _load_state()

    state.update(
        {
            "status": "closed",
            "closed_at": datetime.now(EAT).isoformat(),
        }
    )

    STATE_FILE.write_text(json.dumps(state, indent=2))
    return state


def current_chart_week() -> dict:
    return _load_state()