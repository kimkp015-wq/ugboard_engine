import json
from pathlib import Path
from datetime import datetime
from zoneinfo import ZoneInfo
from typing import Dict, Any

LOG_FILE = Path("data/admin_injection_log.json")
EAT = ZoneInfo("Africa/Kampala")
DAILY_LIMIT = 10


def _now_eat() -> datetime:
    return datetime.now(EAT)


def _today_str() -> str:
    return _now_eat().date().isoformat()


def _default_log(today: str) -> Dict[str, Any]:
    return {
        "date": today,
        "count": 0,
        "events": [],  # audit trail
    }


def _load_log() -> Dict[str, Any]:
    if not LOG_FILE.exists():
        return _default_log(_today_str())

    try:
        data = json.loads(LOG_FILE.read_text())

        # Hard validation (prevents silent reset abuse)
        if not isinstance(data, dict):
            raise ValueError("Invalid log format")

        if "date" not in data or "count" not in data:
            raise ValueError("Missing required fields")

        return data

    except Exception:
        # Corruption detected → rotate file safely
        corrupted = LOG_FILE.with_suffix(".corrupt.json")
        try:
            LOG_FILE.rename(corrupted)
        except Exception:
            pass

        return _default_log(_today_str())


def _save_log(data: Dict[str, Any]) -> None:
    LOG_FILE.parent.mkdir(parents=True, exist_ok=True)

    tmp_file = LOG_FILE.with_suffix(".tmp")
    tmp_file.write_text(json.dumps(data, indent=2))
    tmp_file.replace(LOG_FILE)


# =========================
# PUBLIC API
# =========================

def can_inject_today() -> bool:
    today = _today_str()
    log = _load_log()

    # New day → reset allowed
    if log["date"] != today:
        return True

    return log["count"] < DAILY_LIMIT


def record_injection(meta: Dict[str, Any] | None = None) -> None:
    """
    Records an admin injection.
    Must be called AFTER can_inject_today().
    """

    now = _now_eat()
    today = now.date().isoformat()

    log = _load_log()

    # New day rollover
    if log["date"] != today:
        log = _default_log(today)

    log["count"] += 1

    log["events"].append({
        "timestamp": now.isoformat(),
        "meta": meta or {},
    })

    _save_log(log)