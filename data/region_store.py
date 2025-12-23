# data/region_store.py

import json
from pathlib import Path
from datetime import datetime
from zoneinfo import ZoneInfo
from typing import Dict

EAT = ZoneInfo("Africa/Kampala")

STATE_FILE = Path("data/region_state.json")
INDEX_FILE = Path("data/index.json")

VALID_REGIONS = ("Eastern", "Northern", "Western")


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
# Region state logic
# -------------------------
def _load_state() -> Dict:
    """
    Load region state from disk.
    Safe: never raises.
    """
    return _load_json(STATE_FILE)


def _save_state(state: Dict) -> None:
    _save_json(STATE_FILE, state)


def is_region_locked(region: str) -> bool:
    if region not in VALID_REGIONS:
        return False

    state = _load_state()
    return state.get(region, {}).get("status") == "locked"


def lock_region(region: str) -> Dict:
    """
    Lock a region chart.
    Idempotent and safe.
    Also updates index.json
    """
    if region not in VALID_REGIONS:
        raise ValueError("Invalid region")

    state = _load_state()

    # Already locked → no-op
    if state.get(region, {}).get("status") == "locked":
        return state[region]

    region_state = {
        "status": "locked",
        "locked_at": _now(),
    }

    state[region] = region_state
    _save_state(state)

    _update_index(
        {
            f"region_{region.lower()}": "locked",
            f"region_{region.lower()}_locked_at": region_state["locked_at"],
        }
    )

    return region_state


def unlock_region(region: str) -> Dict:
    """
    Unlock a region (admin/internal only).
    Also updates index.json
    """
    if region not in VALID_REGIONS:
        raise ValueError("Invalid region")

    state = _load_state()

    region_state = {
        "status": "unlocked",
        "unlocked_at": _now(),
    }

    state[region] = region_state
    _save_state(state)

    _update_index(
        {
            f"region_{region.lower()}": "unlocked",
            f"region_{region.lower()}_unlocked_at": region_state["unlocked_at"],
        }
    )

    return region_state


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
    return _load_state().get(region)