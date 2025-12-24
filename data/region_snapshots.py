# data/region_snapshots.py

import json
from pathlib import Path
from typing import List, Dict, Optional
from data.chart_week import current_chart_week
from data.store import load_items

SNAPSHOT_DIR = Path("data/region_snapshots")
VALID_REGIONS = ("Eastern", "Northern", "Western")


def _get_week_id() -> str:
    week = current_chart_week()
    return week.get("week_id", "unknown-week")


def _snapshot_path(region: str, week_id: str) -> Path:
    return SNAPSHOT_DIR / week_id / f"{region.lower()}.json"


def save_region_snapshot(region: str) -> Dict:
    """
    Save Top 5 snapshot for a region for the current chart week.
    Safe, deterministic, week-aware.
    """
    if region not in VALID_REGIONS:
        raise ValueError("Invalid region")

    week_id = _get_week_id()
    path = _snapshot_path(region, week_id)
    path.parent.mkdir(parents=True, exist_ok=True)

    items = load_items()
    region_items = [
        i for i in items
        if i.get("region") == region
    ]

    region_items.sort(
        key=lambda x: x.get("score", 0),
        reverse=True
    )

    snapshot = region_items[:5]

    payload = {
        "week_id": week_id,
        "region": region,
        "count": len(snapshot),
        "items": snapshot,
    }

    path.write_text(json.dumps(payload, indent=2))
    return payload


def load_region_snapshot(region: str) -> Optional[Dict]:
    """
    Load snapshot for current chart week.
    Returns None if not found.
    """
    if region not in VALID_REGIONS:
        return None

    week_id = _get_week_id()
    path = _snapshot_path(region, week_id)

    if not path.exists():
        return None

    try:
        return json.loads(path.read_text())
    except Exception:
        return None