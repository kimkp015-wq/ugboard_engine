# data/admin_injection_log.py

import json
from datetime import date
from pathlib import Path

LOG_FILE = Path("data/admin_injection_log.json")
MAX_DAILY_INJECTIONS = 10


def _load_log():
    if not LOG_FILE.exists():
        return {}
    try:
        return json.loads(LOG_FILE.read_text())
    except Exception:
        return {}


def _save_log(data):
    LOG_FILE.parent.mkdir(parents=True, exist_ok=True)
    LOG_FILE.write_text(json.dumps(data, indent=2))


def get_today_count():
    today = str(date.today())
    log = _load_log()
    return log.get(today, 0)


def can_inject():
    return get_today_count() < MAX_DAILY_INJECTIONS


def record_injection(count=1):
    today = str(date.today())
    log = _load_log()
    log[today] = log.get(today, 0) + count
    _save_log(log)