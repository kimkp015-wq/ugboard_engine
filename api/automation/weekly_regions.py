from datetime import datetime
import pytz

from data.region_publish_state import was_region_published_this_week
from data.region_store import is_region_locked
from data.store import load_items
from data.region_snapshots import save_region_snapshot
from data.region_store import lock_region

REGIONS = ["Eastern", "Northern", "Western"]
EAT = pytz.timezone("Africa/Kampala")


def run_weekly_region_publish():
    """
    Billboard-style weekly automation.
    Safe to run multiple times.
    """
    now = datetime.now(EAT)
    weekday = now.weekday()  # Monday = 0, Sunday = 6

    # Only publish on FRIDAY
    if weekday != 4:
        return {
            "status": "skipped",
            "reason": "Not publishing day",
            "day": weekday
        }

    items = load_items()
    published = []
    skipped = []

    for region in REGIONS:

        # Weekly guard
        if was_region_published_this_week(region):
            skipped.append({
                "region": region,
                "reason": "Already published this week"
            })
            continue

        # Lock guard
        if is_region_locked(region):
            skipped.append({
                "region": region,
                "reason": "Region locked"
            })
            continue

        region_items = [
            i for i in items
            if i.get("region") == region
        ]

        if not region_items:
            skipped.append({
                "region": region,
                "reason": "No songs"
            })
            continue

        top5 = sorted(
            region_items,
            key=lambda x: x.get("score", 0),
            reverse=True
        )[:5]

        save_region_snapshot(region, top5)
        lock_region(region)

        published.append(region)

    return {
        "status": "completed",
        "published": published,
        "skipped": skipped,
        "timestamp": now.isoformat()
    }