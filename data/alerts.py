# data/alerts.py

from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from data.region_store import is_region_locked
from data.chart_week import current_chart_week

EAT = ZoneInfo("Africa/Kampala")

REGIONS = ("Eastern", "Northern", "Western")


def detect_partial_publish():
    """
    Detect if some regions are locked (published)
    while others are not.
    """
    locked = [r for r in REGIONS if is_region_locked(r)]

    if locked and len(locked) < len(REGIONS):
        return {
            "type": "partial_publish",
            "locked_regions": locked,
            "missing_regions": [r for r in REGIONS if r not in locked],
        }

    return None


def detect_scheduler_stall(last_run_iso: str | None):
    """
    Detect if scheduler has not run in expected window.
    """
    if not last_run_iso:
        return {
            "type": "scheduler_never_ran",
        }

    try:
        last_run = datetime.fromisoformat(last_run_iso)

        # Force timezone safety
        if last_run.tzinfo is None:
            last_run = last_run.replace(tzinfo=EAT)

        now = datetime.now(EAT)

        if now - last_run > timedelta(days=7):
            return {
                "type": "scheduler_stalled",
                "last_run": last_run_iso,
            }

    except Exception:
        return {
            "type": "scheduler_invalid_timestamp",
            "value": last_run_iso,
        }

    return None


def collect_alerts(last_scheduler_run: str | None):
    """
    Single alert collection entrypoint.
    SAFE to call anytime.
    """
    alerts = []

    partial = detect_partial_publish()
    if partial:
        alerts.append(partial)

    scheduler = detect_scheduler_stall(last_scheduler_run)
    if scheduler:
        alerts.append(scheduler)

    try:
        week = current_chart_week()
    except Exception:
        week = None

    return {
        "week": week,
        "alerts": alerts,
    }