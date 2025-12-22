import json
from pathlib import Path
from datetime import datetime, timedelta, timezone

STATE_FILE = Path("data/region_publish_state.json")

# Uganda / East Africa Time (UTC+3)
EAT = timezone(timedelta(hours=3))

VALID_REGIONS = ["Eastern", "Northern", "Western"]


def _current_week_key() -> str:
    """
    Returns ISO year-week string in EAT timezone.
    Example: '2025-W52'
    """
    now = datetime.now(EAT)
    year, week, _ = now.isocalendar()
    return f"{year}-W{week}"


def _load_state() -> dict:
    if not STATE_FILE.exists():
        return {}

    try:
        return json.loads(STATE_FILE.read_text())
    except Exception:
        return {}


def _save_state(state: dict):
    STATE_FILE.write_text(json.dumps(state, indent=2))


# -----------------------------------
# Public API (SAFE)
# -----------------------------------

def was_region_published_this_week(region: str) -> bool:
    region = region.title()
    if region not in VALID_REGIONS:
        return False

    state = _load_state()
    current_week = _current_week_key()

    return state.get(region) == current_week


def mark_region_published(region: str):
    region = region.title()
    if region not in VALID_REGIONS:
        return

    state = _load_state()
    state[region] = _current_week_key()
    _save_state(state)