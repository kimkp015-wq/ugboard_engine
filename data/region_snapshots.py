# data/region_snapshots.py

import json
from pathlib import Path
from typing import Dict, Optional

from data.chart_week import current_chart_week
from data.store import load_items

SNAPSHOT_DIR = Path("data/region_snapshots")
VALID_REGIONS = ("Eastern", "Northern", "Western")


# -------------------------
# Helpers
# -------------------------

def _get_week_id() -> str:
    """
    Always return a usable week_id.
    Never returns None.
    """
    week = current_chart_week() or {}
    return week.get("week_id") or "untracked-week"


def _snapshot_path(region: str, week_id: str) -> Path:
    return SNAPSHOT_DIR / week_id / f"{region.lower()}.json"


# -------------------------
# Write snapshot
# -------------------------

def save_region_snapshot(region: str) -> Dict:
    """
    Save Top 5 snapshot for a region for the current chart week.

    Guarantees:
    - Week-aware
    - Idempotent
    - Safe if no items exist
    """
    if region not in VALID_REGIONS:
        raise ValueError("Invalid region")

    week_id = _get_week_id()
    path = _snapshot_path(region, week_id)
    path.parent.mkdir(parents=True, exist_ok=True)

    items = load_items() or []

    region_items = [
        i for i in items
        if i.get("region") == region
    ]

    region_items.sort(
        key=lambda x: x.get("score", 0),
        reverse=True,
    )

    snapshot_items = region_items[:5]

    payload = {
        "week_id": week_id,
        "region": region,
        "count": len(snapshot_items),
        "items": snapshot_items,
    }

    path.write_text(json.dumps(payload, indent=2))
    return payload


# -------------------------
# Read snapshot
# -------------------------

def load_region_snapshot(region: str) -> Optional[Dict]:
    """
    Load snapshot for current chart week.

    Never crashes.
    Returns None only if snapshot truly does not exist.
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