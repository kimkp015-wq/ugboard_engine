from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from data.region_store import is_region_locked
from data.chart_week import current_chart_week

EAT = ZoneInfo("Africa/Kampala")


def detect_scheduler_alerts() -> dict | None:
    """
    Detect scheduler-related failures:
    - Missed weekly publish
    - Partial region publish
    """

    now = datetime.now(EAT)
    week = current_chart_week()

    locked_regions = [
        region
        for region in ("Eastern", "Northern", "Western")
        if is_region_locked(region)
    ]

    # ❌ No region published by expected time (Monday 00:10 EAT)
    if now.weekday() == 0 and now.hour >= 0 and not locked_regions:
        return {
            "type": "missed_publish",
            "week": week,
            "timestamp": now.isoformat(),
        }

    # ⚠️ Partial publish
    if 0 < len(locked_regions) < 3:
        return {
            "type": "partial_publish",
            "week": week,
            "regions_locked": locked_regions,
            "timestamp": now.isoformat(),
        }

    return None