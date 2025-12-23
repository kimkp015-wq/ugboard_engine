# data/alerts.py

from datetime import datetime
from zoneinfo import ZoneInfo
from pathlib import Path
import json

EAT = ZoneInfo("Africa/Kampala")

AUDIT_FILE = Path("data/audit_log.json")
ALERT_FILE = Path("data/alerts_log.json")


def _load_audit() -> list:
    if not AUDIT_FILE.exists():
        return []
    try:
        return json.loads(AUDIT_FILE.read_text())
    except Exception:
        return []


def _save_alert(alert: dict):
    ALERT_FILE.parent.mkdir(parents=True, exist_ok=True)

    existing = []
    if ALERT_FILE.exists():
        try:
            existing = json.loads(ALERT_FILE.read_text())
        except Exception:
            existing = []

    existing.append(alert)
    ALERT_FILE.write_text(json.dumps(existing, indent=2))


# =========================
# MAIN ALERT CHECK
# =========================
def detect_publish_alert():
    """
    Detect missing or partial weekly publish.
    Safe to call multiple times.
    """

    now = datetime.now(EAT)
    today = now.date().isoformat()

    audit = _load_audit()

    published_regions = {
        a["region"]
        for a in audit
        if a.get("action") == "publish_region"
        and a.get("timestamp", "").startswith(today)
    }

    expected = {"Eastern", "Northern", "Western"}

    if published_regions == expected:
        return None  # all good

    alert = {
        "type": "weekly_publish_incomplete",
        "date": today,
        "published": sorted(published_regions),
        "missing": sorted(expected - published_regions),
        "timestamp": now.isoformat(),
    }

    _save_alert(alert)
    return alert