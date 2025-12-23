from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

EAT = ZoneInfo("Africa/Kampala")

TRACKING_START_WEEKDAY = 0  # Monday
TRACKING_END_WEEKDAY = 3    # Thursday


def current_chart_week() -> str:
    """
    Returns chart week identifier (YYYY-WW) in EAT timezone.
    """
    now = datetime.now(EAT)
    year, week, _ = now.isocalendar()
    return f"{year}-W{week:02d}"


def is_tracking_open() -> bool:
    """
    Tracking window:
    Monday 00:00 EAT → Thursday 23:59 EAT
    """
    now = datetime.now(EAT)
    weekday = now.weekday()

    if TRACKING_START_WEEKDAY <= weekday <= TRACKING_END_WEEKDAY:
        return True

    return False