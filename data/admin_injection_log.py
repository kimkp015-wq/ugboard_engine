import json
import os
from datetime import datetime, timedelta, timezone
from typing import List, Dict

LOG_FILE = "data/admin_injection_log.json"
DAILY_LIMIT = 10

# ✅ East Africa Time (UTC+3)
EAT = timezone(timedelta(hours=3))


def _load_log() -> List[Dict]:
    if not os.path.exists(LOG_FILE):
        return []

    try:
        with open(LOG_FILE, "r") as f:
            data = json.load(f)
            return data if isinstance(data, list) else []
    except Exception:
        # Never crash engine due to log issues
        return []


def _save_log(entries: List[Dict]):
    os.makedirs(os.path.dirname(LOG_FILE), exist_ok=True)
    with open(LOG_FILE, "w") as f:
        json.dump(entries, f, indent=2)


def _today_eat() -> str:
    return datetime.now(EAT).date().isoformat()


# 🔒 HARD DAILY LIMIT (EAT-BASED)
def can_inject_today() -> bool:
    today = _today_eat()
    log = _load_log()

    today_count = sum(
        1 for entry in log if entry.get("date") == today
    )

    return today_count < DAILY_LIMIT


# 🧾 APPEND-ONLY LOG (AUDIT SAFE)
def log_admin_injection(
    title: str,
    artist: str,
    region: str | None
):
    log = _load_log()

    entry = {
        "title": title,
        "artist": artist,
        "region": region,
        "date": _today_eat(),
        "timestamp": datetime.now(EAT).isoformat()
    }

    log.append(entry)
    _save_log(log)