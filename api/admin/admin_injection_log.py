# data/admin_injection_log.py

import json
from pathlib import Path
from datetime import date

LOG_PATH = Path("data/admin_injection_log.json")
DAILY_LIMIT = 10


def _load():
    if not LOG_PATH.exists():
        return {}
    try:
        return json.loads(LOG_PATH.read_text())
    except Exception:
        return {}


def _save(data):
    LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    LOG_PATH.write_text(json.dumps(data, indent=2))


def can_inject_today() -> bool:
    data = _load()
    today = str(date.today())
    return len(data.get(today, [])) < DAILY_LIMIT


def log_injection(song: dict):
    data = _load()
    today = str(date.today())

    data.setdefault(today, []).append({
        "title": song.get("title"),
        "artist": song.get("artist"),
        "region": song.get("region")
    })

    _save(data)


def injections_today() -> int:
    data = _load()
    today = str(date.today())
    return len(data.get(today, []))