# data/alerts.py

from typing import Optional
from data.chart_week import (
    now_eat,
    is_publish_window,
    is_frozen_period,
    chart_week_label
)
from data.audit import get_last_publish_event


def detect_missed_publish() -> Optional[str]:
    """
    Detects if a weekly publish was missed.
    Returns an alert message string if detected, else None.
    """

    # Only evaluate AFTER publish window has closed
    if is_publish_window():
        return None

    if not is_frozen_period():
        return None

    week = chart_week_label()
    last_publish = get_last_publish_event(week)

    if last_publish is None:
        return (
            f"[ALERT] Weekly publish missing for chart week {week}. "
            "No successful publish event recorded."
        )

    if last_publish.get("status") != "success":
        return (
            f"[ALERT] Weekly publish incomplete for chart week {week}. "
            f"Last status: {last_publish.get('status')}"
        )

    return None