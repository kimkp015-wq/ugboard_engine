# data/region_store.py

import json
from pathlib import Path
from datetime import datetime, timezone, timedelta

REGION_FILE = Path("data/region_charts.json")
REGIONS = ["Eastern", "Northern", "Western"]


def _eat_now():
    return datetime.now(timezone(timedelta(hours=3)))


def current_week_key():
    now = _eat_now()
    year, week, _ = now.isocalendar()
    return f"{year}-W{week}"


def load_region_charts():
    if not REGION_FILE.exists():
        return {}

    try:
        return json.loads(REGION_FILE.read_text())
    except Exception:
        return {}


def save_region_charts(data):
    REGION_FILE.parent.mkdir(parents=True, exist_ok=True)
    REGION_FILE.write_text(json.dumps(data, indent=2))


def is_region_locked(region):
    charts = load_region_charts()
    region_data = charts.get(region)
    if not region_data:
        return False

    return region_data.get("locked") is True


def publish_region(region, items):
    charts = load_region_charts()

    if region in charts and charts[region].get("locked"):
        raise ValueError("Region already locked for this week")

    charts[region] = {
        "week": current_week_key(),
        "locked": True,
        "items": items
    }

    save_region_charts(charts)


def get_region(region):
    charts = load_region_charts()
    return charts.get(region)