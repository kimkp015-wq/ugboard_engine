# data/alerts.py

from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from pathlib import Path
import json

EAT = ZoneInfo("Africa/Kampala")

PUBLISH_LOG = Path("data/publish_log.json")


def _load_publish_log() -> dict:
    if not PUBLISH_LOG.exists():
        return {}

    try:
        return json.loads(PUBLISH_LOG.read_text())
    except Exception:
        return {}


def detect_missed_publish():
    """
    Detect if weekly publish did not happen.
    SAFE: returns None if no alert.
    NEVER raises.
    """
    try:
        now = datetime.now(EAT)

        # Expect publish by Monday 09:00 EAT
        expected_time = now.replace(
            hour=9, minute=0, second=0, microsecond=0
        )

        if now.weekday() != 0:  # 0 = Monday
            return None

        log = _load_publish_log()
        last_publish = log.get("last_publish")

        if not last_publish:
            return "No publish record found for this week"

        last_dt = datetime.fromisoformat(last_publish)

        if last_dt < expected_time:
            return "Weekly publish missed"

        return None

    except Exception as e:
        # NEVER crash health
        return f"Alert check failed safely: {e}"