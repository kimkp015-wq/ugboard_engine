# data/admin_injection_log.py

import json
import os
from datetime import date

LOG_PATH = "data/admin_injection_log.json"
DAILY_LIMIT = 10


def _load():
    if not os.path.exists(LOG_PATH):
        return {}
    with open(LOG_PATH, "r") as f:
        return json.load(f)


def _save(data):
    os.makedirs("data", exist_ok=True)
    with open(LOG_PATH, "w") as f:
        json.dump(data, f, indent=2)


def can_inject_today() -> bool:
    data = _load()
    today = date.today().isoformat()
    return data.get(today, 0) < DAILY_LIMIT


def record_injection(count: int = 1):
    data = _load()
    today = date.today().isoformat()
    data[today] = data.get(today, 0) + count
    _save(data)


def injections_today() -> int:
    data = _load()
    today = date.today().isoformat()
    return data.get(today, 0)