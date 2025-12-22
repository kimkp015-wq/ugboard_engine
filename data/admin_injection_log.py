# data/admin_injection_log.py

import json
from datetime import date
from pathlib import Path
from typing import List, Dict

LOG_FILE = Path("data/admin_injection_log.json")
DAILY_LIMIT = 10


def _load_log() -> List[Dict]:
    if not LOG_FILE.exists():
        return []
    try:
        return json.loads(LOG_FILE.read_text())
    except Exception:
        return []


def _save_log(entries: List[Dict]):
    LOG_FILE.parent.mkdir(parents=True, exist_ok=True)
    LOG_FILE.write_text(json.dumps(entries, indent=2))


def can_inject_today() -> bool:
    """Check if admin is still allowed to inject songs today"""
    today = date.today().isoformat()
    entries = _load_log()
    today_count = sum(1 for e in entries if e.get("date") == today)
    return today_count < DAILY_LIMIT


def record_injection(title: str, artist: str, region: str):
    """Record an admin injection (audit-safe)"""
    entries = _load_log()
    entries.append({
        "date": date.today().isoformat(),
        "title": title,
        "artist": artist,
        "region": region
    })
    _save_log(entries)


def injections_today() -> int:
    today = date.today().isoformat()
    entries = _load_log()
    return sum(1 for e in entries if e.get("date") == today)