# data/scheduler_state.py

import json
from pathlib import Path
from datetime import datetime
from zoneinfo import ZoneInfo
from typing import Dict

EAT = ZoneInfo("Africa/Kampala")
STATE_FILE = Path("data/scheduler_state.json")


def _now() -> str:
    return datetime.now(EAT).isoformat()


def _load_state() -> Dict:
    """
    Load scheduler state from disk.
    Safe: never raises.
    """
    if not STATE_FILE.exists():
        return {}

    try:
        return json.loads(STATE_FILE.read_text())
    except Exception:
        return {}


def record_scheduler_run(trigger: str = "unknown") -> Dict:
    """
    Record a successful scheduler run.

    trigger examples:
    - cron
    - cloudflare_worker
    - admin_manual
    """
    STATE_FILE.parent.mkdir(parents=True, exist_ok=True)

    state = {
        "last_run_at": _now(),
        "trigger": trigger,
    }

    STATE_FILE.write_text(json.dumps(state, indent=2))
    return state


def get_last_scheduler_run() -> Dict | None:
    """
    Return last scheduler run info.
    Returns None if never run.
    """
    state = _load_state()
    return state if state else None