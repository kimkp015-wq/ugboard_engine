import json
from pathlib import Path
from datetime import datetime
from zoneinfo import ZoneInfo

LOG_FILE = Path("data/admin_injection_log.json")
EAT = ZoneInfo("Africa/Kampala")
DAILY_LIMIT = 10


def _load_log() -> dict:
    if not LOG_FILE.exists():
        return {}
    try:
        return json.loads(LOG_FILE.read_text())
    except Exception:
        return {}


def _save_log(data: dict):
    LOG_FILE.write_text(json.dumps(data, indent=2))


def _today_key() -> str:
    return datetime.now(EAT).date().isoformat()


def injections_today() -> int:
    log = _load_log()
    today = _today_key()
    return int(log.get(today, {}).get("count", 0))


def can_inject_today() -> bool:
    return injections_today() < DAILY_LIMIT


def record_injection(song: dict):
    """
    Records a successful admin injection.
    """
    log = _load_log()
    today = _today_key()

    if today not in log:
        log[today] = {
            "count": 0,
            "items": []
        }

    log[today]["count"] += 1
    log[today]["items"].append({
        "title": song.get("title"),
        "artist": song.get("artist"),
        "region": song.get("region"),
        "timestamp": datetime.now(EAT).isoformat()
    })

    _save_log(log)