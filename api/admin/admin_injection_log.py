import json
from pathlib import Path
from datetime import datetime
from zoneinfo import ZoneInfo

LOG_FILE = Path("data/admin_injection_log.json")
EAT = ZoneInfo("Africa/Kampala")
DAILY_LIMIT = 10


def _load():
    if not LOG_FILE.exists():
        return {}
    try:
        return json.loads(LOG_FILE.read_text())
    except Exception:
        return {}


def _save(data: dict):
    LOG_FILE.write_text(json.dumps(data, indent=2))


def _today():
    return datetime.now(EAT).date().isoformat()


def injections_today() -> int:
    data = _load()
    return int(data.get(_today(), {}).get("count", 0))


def can_inject_today() -> bool:
    return injections_today() < DAILY_LIMIT


def record_injection(song: dict):
    data = _load()
    today = _today()

    if today not in data:
        data[today] = {"count": 0, "items": []}

    data[today]["count"] += 1
    data[today]["items"].append({
        "title": song.get("title"),
        "artist": song.get("artist"),
        "region": song.get("region"),
        "time": datetime.now(EAT).isoformat()
    })

    _save(data)