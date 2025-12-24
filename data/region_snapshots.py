import json
from pathlib import Path
from typing import Dict, Optional

from data.chart_week import current_chart_week

SNAPSHOT_DIR = Path("data/region_snapshots")
VALID_REGIONS = ("Eastern", "Northern", "Western")


# -------------------------
# Internal helpers
# -------------------------

def _get_week_id() -> str:
    week = current_chart_week()
    return week.get("week_id", "unknown-week")


def _snapshot_path(region: str, week_id: str) -> Path:
    return SNAPSHOT_DIR / week_id / f"{region.lower()}.json"


def _load_items_safe():
    """
    Lazy import to prevent startup crashes.
    """
    try:
        from data.store import load_items
        return load_items()
    except Exception:
        return []


# -------------------------
# Public API
# -------------------------

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

    items = _load_items_safe()

    region_items = [
        i for i in items
        if i.get("region") == region
    ]

    region_items.sort(
        key=lambda x: x.get("score", 0),
        reverse=True
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