# data/chart_week.py

from datetime import datetime, time, timedelta
from zoneinfo import ZoneInfo

EAT = ZoneInfo("Africa/Nairobi")


def now_eat() -> datetime:
    """Current time in EAT"""
    return datetime.now(tz=EAT)


def start_of_week(dt: datetime) -> datetime:
    """
    Returns Monday 00:00 EAT of the current chart week
    """
    monday = dt - timedelta(days=dt.weekday())
    return datetime.combine(
        monday.date(),
        time(0, 0),
        tzinfo=EAT
    )


def is_tracking_open() -> bool:
    """
    True if we are within Mon–Thu tracking window
    """
    now = now_eat()
    return now.weekday() in (0, 1, 2, 3)  # Mon–Thu


def is_publish_window() -> bool:
    """
    True if we are in Friday publish window
    """
    now = now_eat()
    return now.weekday() == 4 and now.hour < 3  # Friday 00:00–02:59


def is_frozen_period() -> bool:
    """
    True if charts must not change (Fri–Sun)
    """
    now = now_eat()
    return now.weekday() in (4, 5, 6)  # Fri–Sun


def chart_week_label() -> str:
    """
    Human-readable label, e.g. '2025-W52'
    """
    start = start_of_week(now_eat())
    year, week, _ = start.isocalendar()
    return f"{year}-W{week}"