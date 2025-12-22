# data/admin_injection_log.py

import json
from pathlib import Path
from datetime import date

LOG_PATH = Path("data/admin_injection_log.json")
DAILY_LIMIT = 10


def _load_log() -> dict:
    """
    Internal helper.
    Always returns a valid dict.
    Never crashes.
    """
    if not LOG_PATH.exists():
        return {}

    try:
        return json.loads(LOG_PATH.read_text())
    except Exception:
        # Corrupt file safety
        return {}


def _save_log(data: dict) -> None:
    LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    LOG_PATH.write_text(json.dumps(data, indent=2))


def _today_key() -> str:
    return date.today().isoformat()


def read_injections_today() -> int:
    """
    Returns how many admin injections
    have happened today.
    """
    log = _load_log()
    return int(log.get(_today_key(), 0))


def can_inject_today(requested: int = 1) -> bool:
    """
    Checks if admin can inject `requested` items today.
    Enforces DAILY_LIMIT strictly.
    """
    today_count = read_injections_today()
    return (today_count + requested) <= DAILY_LIMIT


def record_injection(count: int) -> None:
    """
    Records successful admin injections.
    Assumes validation already passed.
    """
    if count <= 0:
        return

    log = _load_log()
    today = _today_key()

    log[today] = int(log.get(today, 0)) + int(count)
    _save_log(log)