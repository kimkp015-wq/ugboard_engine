from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
import json
from pathlib import Path

LOG_FILE = Path("data/admin_injection_log.json")
EAT = ZoneInfo("Africa/Kampala")
DAILY_LIMIT = 10


def _load():
    if not LOG_FILE.exists():
        return []
    return json.loads(LOG_FILE.read_text())


def _save(entries):
    LOG_FILE.write_text(json.dumps(entries, indent=2))


def can_inject_today() -> bool:
    today = datetime.now(EAT).date()
    entries = _load()
    today_count = sum(
        1 for e in entries
        if datetime.fromisoformat(e["timestamp"]).date() == today
    )
    return today_count < DAILY_LIMIT


def log_injection(song_id: str, region: str, admin: str):
    entries = _load()
    entries.append({
        "song_id": song_id,
        "region": region.lower(),
        "admin": admin,
        "timestamp": datetime.now(EAT).isoformat()
    })
    _save(entries)