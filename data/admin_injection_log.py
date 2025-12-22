# data/admin_injection_log.py

import json
from pathlib import Path
from datetime import datetime, timedelta, timezone

# -----------------------------
# Configuration
# -----------------------------
LOG_FILE = Path("data/admin_injection_log.json")
DAILY_LIMIT = 10

# East Africa Time (UTC+3)
EAT = timezone(timedelta(hours=3))


# -----------------------------
# Internal helpers (SAFE)
# -----------------------------
def _load_log():
    """
    Load admin injection log safely.
    Never crashes.
    """
    if not LOG_FILE.exists():
        return []

    try:
        return json.loads(LOG_FILE.read_text())
    except Exception:
        # Corrupt or unreadable log → treat as empty
        return []


def _save_log(entries):
    """
    Persist admin injection log safely.
    """
    LOG_FILE.parent.mkdir(parents=True, exist_ok=True)
    LOG_FILE.write_text(json.dumps(entries, indent=2))


def _today_eat():
    """
    Returns today's date string in EAT.
    """
    return datetime.now(EAT).date().isoformat()


def _now_eat_iso():
    """
    Returns current timestamp in ISO format with EAT offset.
    """
    return datetime.now(EAT).isoformat()


# -----------------------------
# Public API (USED BY ADMIN)
# -----------------------------
def injections_today():
    """
    Returns number of admin injections made today (EAT).
    """
    today = _today_eat()
    log = _load_log()

    return sum(1 for entry in log if entry.get("date") == today)


def can_inject_today():
    """
    Returns True if admin can still inject today.
    Enforces HARD 10/day limit.
    """
    return injections_today() < DAILY_LIMIT


def remaining_injections_today():
    """
    Returns remaining admin injections allowed today.
    """
    used = injections_today()
    remaining = DAILY_LIMIT - used
    return max(0, remaining)


def log_admin_injection(
    *,
    title: str,
    artist: str,
    region: str
):
    """
    Append a new admin injection entry to the audit log.

    Assumes validation is already done at API layer.
    """
    log = _load_log()

    entry = {
        "date": _today_eat(),
        "timestamp": _now_eat_iso(),
        "title": title,
        "artist": artist,
        "region": region
    }

    log.append(entry)
    _save_log(log)