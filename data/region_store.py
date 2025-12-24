# data/region_store.py

import json
from pathlib import Path
from datetime import datetime
from zoneinfo import ZoneInfo
from typing import Dict, Optional

from data.chart_week import current_chart_week

EAT = ZoneInfo("Africa/Kampala")

STATE_DIR = Path("data/region_state")
INDEX_FILE = Path("data/index.json")

VALID_REGIONS = ("Eastern", "Northern", "Western")


# =========================
# Helpers
# =========================

def _now() -> str:
    return datetime.now(EAT).isoformat()


def _atomic_write(path: Path, data: Dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(data, indent=2))
    tmp.replace(path)


def _load_json(path: Path) -> Dict:
    if not path.exists():
        return {}
    try:
        data = json.loads(path.read_text())
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def _get_week_id() -> str:
    week = current_chart_week()
    week_id = week.get("week_id")

    if not isinstance(week_id, str) or not week_id:
        raise RuntimeError("Chart week not initialized")

    return week_id


def _state_file() -> Path:
    """
    Week-scoped region state file.
    """
    return STATE_DIR / f"{_get_week_id()}.json"


# =========================
# Index handling (append-only)
# =========================

def _append_index(entry: Dict) -> None:
    index = _load_json(INDEX_FILE)

    entries = index.get("entries")
    if not isinstance(entries, list):
        entries = []

    entries.append(entry)
    index["entries"] = entries

    _atomic_write(INDEX_FILE, index)


# =========================
# Region state logic
# =========================

def _load_state() -> Dict:
    return _load_json(_state_file())


def _save_state(state: Dict) -> None:
    _atomic_write(_state_file(), state)


def is_region_locked(region: str) -> bool:
    if region not in VALID_REGIONS:
        return False

    state = _load_state()
    return state.get(region, {}).get("status") == "locked"


def lock_region(region: str) -> Dict:
    """
    Lock a region for the current chart week.
    Idempotent, week-scoped, audited.
    """
    if region not in VALID_REGIONS:
        raise ValueError(f"Invalid region: {region}")

    state = _load_state()

    # Idempotent
    if state.get(region, {}).get("status") == "locked":
        return state[region]

    region_state = {
        "status": "locked",
        "locked_at": _now(),
    }

    state[region] = region_state
    _save_state(state)

    _append_index(
        {
            "type": "region_lock",
            "region": region,
            "week_id": _get_week_id(),
            "timestamp": region_state["locked_at"],
        }
    )

    return region_state


def unlock_region(region: str) -> Dict:
    """
    Unlock a region (admin/internal only).
    Preserves audit history.
    """
    if region not in VALID_REGIONS:
        raise ValueError(f"Invalid region: {region}")

    state = _load_state()
    prev = state.get(region, {})

    region_state = {
        **prev,
        "status": "unlocked",
        "unlocked_at": _now(),
    }

    state[region] = region_state
    _save_state(state)

    _append_index(
        {
            "type": "region_unlock",
            "region": region,
            "week_id": _get_week_id(),
            "timestamp": region_state["unlocked_at"],
        }
    )

    return region_state


def any_region_locked() -> bool:
    state = _load_state()
    return any(
        isinstance(v, dict) and v.get("status") == "locked"
        for v in state.values()
    )


def get_region_state(region: str) -> Optional[Dict]:
    if region not in VALID_REGIONS:
        return None
    return _load_state().get(region)