# data/scheduler_state.py

import json
from pathlib import Path
from datetime import datetime
from zoneinfo import ZoneInfo
from typing import Dict, Optional
import tempfile
import os

EAT = ZoneInfo("Africa/Kampala")

STATE_FILE = Path("data/scheduler_state.json")


# -------------------------
# Helpers
# -------------------------

def _now() -> str:
    """Current timestamp in EAT (ISO-8601)."""
    return datetime.now(EAT).isoformat()


def _safe_read_json(path: Path) -> Dict:
    """
    Safe JSON read.
    Never raises.
    Always returns a dict.
    """
    if not path.exists():
        return {}

    try:
        data = json.loads(path.read_text())
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def _atomic_write(path: Path, data: Dict) -> None:
    """
    Atomic JSON write.
    Prevents partial file corruption.
    """
    path.parent.mkdir(parents=True, exist_ok=True)

    tmp_fd, tmp_path = tempfile.mkstemp(dir=path.parent)
    try:
        with os.fdopen(tmp_fd, "w") as f:
            json.dump(data, f, indent=2)
        os.replace(tmp_path, path)
    finally:
        if os.path.exists(tmp_path):
            os.unlink(tmp_path)


# -------------------------
# Scheduler state (ISOLATED)
# -------------------------

def _load_state() -> Dict:
    """
    Load scheduler state from disk.
    Safe: never raises.
    """
    return _safe_read_json(STATE_FILE)


def record_scheduler_run(trigger: str = "unknown") -> Dict:
    """
    Record a successful scheduler or admin-triggered run.

    This state is:
    - Informational only
    - NOT part of chart index
    - Safe to overwrite
    """
    state = {
        "last_run_at": _now(),
        "trigger": trigger,
    }

    _atomic_write(STATE_FILE, state)
    return state


def get_last_scheduler_run() -> Optional[Dict]:
    """
    Return last scheduler run info.
    Returns None if never run.
    """
    state = _load_state()
    return state if state else None