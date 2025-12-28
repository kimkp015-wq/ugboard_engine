import json
from pathlib import Path
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from typing import Dict

EAT = ZoneInfo("Africa/Kampala")
STATE_FILE = Path("data/rate_limit_state.json")

WINDOW_MINUTES = 10
MAX_BATCHES = 6
MAX_ITEMS = 50


def _now() -> datetime:
    return datetime.now(EAT)


def _load() -> Dict:
    if not STATE_FILE.exists():
        return {}

    try:
        data = json.loads(STATE_FILE.read_text())
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def _save(state: Dict) -> None:
    STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    tmp = STATE_FILE.with_suffix(".tmp")
    tmp.write_text(json.dumps(state, indent=2))
    tmp.replace(STATE_FILE)


def check_and_record(source: str, batch_size: int) -> None:
    """
    Enforce rate limits per source.
    Raises RuntimeError if violated.
    """
    if batch_size > MAX_ITEMS:
        raise RuntimeError("Batch too large")

    state = _load()
    now = _now()
    window_start = now - timedelta(minutes=WINDOW_MINUTES)

    history = state.get(source, [])

    # keep only recent
    history = [
        ts for ts in history
        if datetime.fromisoformat(ts) >= window_start
    ]

    if len(history) >= MAX_BATCHES:
        raise RuntimeError("Rate limit exceeded")

    history.append(now.isoformat())
    state[source] = history

    _save(state)