# data/region_store.py

import json
from pathlib import Path
from datetime import datetime
from zoneinfo import ZoneInfo
from typing import Dict

EAT = ZoneInfo("Africa/Kampala")
STATE_FILE = Path("data/region_state.json")

VALID_REGIONS = ("Eastern", "Northern", "Western")


def _now() -> str:
    return datetime.now(EAT).isoformat()


def _load_state() -> Dict:
    """
    Load region state from disk.
    Safe: never raises.
    """
    if not STATE_FILE.exists():
        return {}

    try:
        return json.loads(STATE_FILE.read_text())
    except Exception:
        return {}


def _save_state(state: Dict) -> None:
    STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    STATE_FILE.write_text(json.dumps(state, indent=2))


def is_region_locked(region: str) -> bool:
    if region not in VALID_REGIONS:
        return False

    state = _load_state()
    region_state = state.get(region, {})
    return region_state.get("status") == "locked"


def lock_region(region: str) -> Dict:
    """
    Lock a region chart.
    Idempotent and safe.
    """
    if region not in VALID_REGIONS:
        raise ValueError("Invalid region")

    state = _load_state()

    # If already locked, do nothing
    if state.get(region, {}).get("status") == "locked":
        return state[region]

    state[region] = {
        "status": "locked",
        "locked_at": _now(),
    }

    _save_state(state)
    return state[region]


def unlock_region(region: str) -> Dict:
    """
    Unlock a region (admin/internal only).
    """
    if region not in VALID_REGIONS:
        raise ValueError("Invalid region")

    state = _load_state()

    state[region] = {
        "status": "unlocked",
        "unlocked_at": _now(),
    }

    _save_state(state)
    return state[region]


def any_region_locked() -> bool:
    """
    Returns True if any region is locked.
    """
    state = _load_state()
    return any(
        region_state.get("status") == "locked"
        for region_state in state.values()
        if isinstance(region_state, dict)
    )


def get_region_state(region: str) -> Dict | None:
    """
    Return raw region state.
    """
    state = _load_state()
    return state.get(region)