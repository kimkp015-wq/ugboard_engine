import json
from pathlib import Path

STATE_FILE = Path("data/region_state.json")

VALID_REGIONS = ["Eastern", "Northern", "Western"]


def _load_state() -> dict:
    if not STATE_FILE.exists():
        return {}
    try:
        return json.loads(STATE_FILE.read_text())
    except Exception:
        return {}


def _save_state(state: dict) -> None:
    STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    STATE_FILE.write_text(json.dumps(state, indent=2))


def is_region_locked(region: str) -> bool:
    state = _load_state()
    return state.get(region) == "locked"


def lock_region(region: str) -> None:
    if region not in VALID_REGIONS:
        raise ValueError("Invalid region")

    state = _load_state()
    state[region] = "locked"
    _save_state(state)


def any_region_locked() -> bool:
    state = _load_state()
    return any(v == "locked" for v in state.values())