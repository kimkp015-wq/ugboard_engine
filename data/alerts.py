# data/alerts.py

from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from data.region_store import is_region_locked
from data.chart_week import last_publish_date

EAT = ZoneInfo("Africa/Kampala")


def detect_missed_publish():
    """
    Detects if a weekly publish was missed.
    Returns alert dict OR None.
    NEVER raises.
    """
    try:
        now = datetime.now(EAT)

        # Expect publish by Monday 23:59 EAT
        deadline = now.replace(hour=23, minute=59, second=0, microsecond=0)

        if now.weekday() > 0:  # After Monday
            last_publish = last_publish_date()

            if not last_publish:
                return {
                    "type": "missed_publish",
                    "severity": "critical",
                    "message": "No publish recorded this week",
                }

            if last_publish < deadline - timedelta(days=1):
                return {
                    "type": "late_publish",
                    "severity": "warning",
                    "message": "Publish occurred late",
                }

        return None

    except Exception as e:
        return {
            "type": "alert_error",
            "severity": "degraded",
            "message": str(e),
        }