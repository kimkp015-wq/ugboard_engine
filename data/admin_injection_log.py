# data/admin_injection_log.py

import json
from pathlib import Path
from datetime import datetime
from zoneinfo import ZoneInfo

LOG_FILE = Path("data/admin_injection_log.json")
EAT = ZoneInfo("Africa/Kampala")
DAILY_LIMIT = 10


def _load_log() -> dict:
    if not LOG_FILE.exists():
        return {"date": None, "count": 0}

    try:
        return json.loads(LOG_FILE.read_text())
    except Exception:
        return {"date": None, "count": 0}


def _save_log(data: dict):
    LOG_FILE.parent.mkdir(parents=True, exist_ok=True)
    LOG_FILE.write_text(json.dumps(data, indent=2))


def can_inject_today() -> bool:
    now = datetime.now(EAT)
    today = now.date().isoformat()

    log = _load_log()

    if log.get("date") != today:
        return True

    return log.get("count", 0) < DAILY_LIMIT


def record_injection():
    now = datetime.now(EAT)
    today = now.date().isoformat()

    log = _load_log()

    if log.get("date") != today:
        log = {"date": today, "count": 1}
    else:
        log["count"] = log.get("count", 0) + 1

    _save_log(log)