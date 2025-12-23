# data/scheduler_state.py

import json
from pathlib import Path
from datetime import datetime
from zoneinfo import ZoneInfo
from typing import Dict

EAT = ZoneInfo("Africa/Kampala")

STATE_FILE = Path("data/scheduler_state.json")
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
    index = _load_json(INDEX_FILE)
    index.update(update)
    _save_json(INDEX_FILE, index)


# -------------------------
# Scheduler state
# -------------------------
def _load_state() -> Dict:
    """
    Load scheduler state from disk.
    Safe: never raises.
    """
    return _load_json(STATE_FILE)


def record_scheduler_run(trigger: str = "unknown") -> Dict:
    """
    Record a successful scheduler run.

    trigger examples:
    - cron
    - cloudflare_worker
    - admin_manual
    """
    state = {
        "last_run_at": _now(),
        "trigger": trigger,
    }

    _save_json(STATE_FILE, state)

    # Sync to index for observability
    _update_index(
        {
            "last_scheduler_run_at": state["last_run_at"],
            "last_scheduler_trigger": state["trigger"],
        }
    )

    return state


def get_last_scheduler_run() -> Dict | None:
    """
    Return last scheduler run info.
    Returns None if never run.
    """
    state = _load_state()
    return state if state else None