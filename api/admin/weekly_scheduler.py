# api/admin/weekly_scheduler.py

from datetime import datetime
from zoneinfo import ZoneInfo
from typing import Dict, List

# IMPORTANT:
# - No FastAPI imports here
# - No chart routers
# - No auto_recalc
# - Only stable data-layer functions


EAT = ZoneInfo("Africa/Kampala")

VALID_REGIONS = ["Eastern", "Northern", "Western"]


def _now_eat() -> datetime:
    """
    Single source of time truth (EAT).
    """
    return datetime.now(tz=EAT)


def _should_publish(now: datetime) -> bool:
    """
    Weekly publish rule:
    Friday at 10:00 EAT
    """
    return (
        now.weekday() == 4 and   # Friday (Mon=0)
        now.hour == 10 and
        now.minute == 0
    )


def _should_unlock(now: datetime) -> bool:
    """
    Weekly reset rule:
    Monday at 00:00 EAT
    """
    return (
        now.weekday() == 0 and   # Monday
        now.hour == 0 and
        now.minute == 0
    )


def _publish_regions() -> Dict:
    """
    Publish & freeze all regions safely.
    Idempotent by design.
    """
    from data.store import load_items
    from data.region_store import is_region_locked, lock_region
    from data.region_snapshots import save_region_snapshot

    items = load_items()
    published: List[str] = []

    for region in VALID_REGIONS:
        if is_region_locked(region):
            continue

        region_items = [
            i for i in items
            if i.get("region") == region
        ]

        region_items.sort(
            key=lambda x: x.get("score", 0),
            reverse=True
        )

        top5 = region_items[:5]

        if not top5:
            continue

        save_region_snapshot(region, top5)
        lock_region(region)
        published.append(region)

    return {
        "action": "publish",
        "regions": published
    }


def _unlock_regions() -> Dict:
    """
    Unlock all regions (weekly reset).
    Safe to rerun.
    """
    from data.region_store import is_region_locked, unlock_region

    unlocked: List[str] = []

    for region in VALID_REGIONS:
        if not is_region_locked(region):
            continue

        unlock_region(region)
        unlocked.append(region)

    return {
        "action": "unlock",
        "regions": unlocked
    }


def run_weekly_scheduler() -> Dict:
    """
    Entry point.
    This is what cron / Cloudflare / Railway calls.
    """
    now = _now_eat()

    if _should_publish(now):
        result = _publish_regions()
    elif _should_unlock(now):
        result = _unlock_regions()
    else:
        result = {
            "action": "none",
            "regions": []
        }

    return {
        "status": "ok",
        "time_eat": now.isoformat(),
        **result
    }