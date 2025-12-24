# data/region_snapshots.py

import json
from pathlib import Path
from typing import Dict, Optional, List

from data.chart_week import current_chart_week

SNAPSHOT_DIR = Path("data/region_snapshots")
VALID_REGIONS = ("Eastern", "Northern", "Western")


# =========================
# Internal helpers
# =========================

def _get_week_id() -> str:
    """
    Resolve current chart week ID.
    Raises if week is not initialized (engine invariant).
    """
    week = current_chart_week()
    week_id = week.get("week_id")

    if not isinstance(week_id, str) or not week_id:
        raise RuntimeError("Chart week not initialized")

    return week_id


def _snapshot_path(region: str, week_id: str) -> Path:
    return SNAPSHOT_DIR / week_id / f"{region.lower()}.json"


def _load_items_safe() -> List[Dict]:
    """
    Lazy import to prevent startup crashes.
    Always returns a list.
    """
    try:
        from data.store import load_items
        items = load_items()
        return items if isinstance(items, list) else []
    except Exception:
        return []


# =========================
# Public API
# =========================

def save_region_snapshot(region: str) -> Dict:
    """
    Save immutable Top 5 snapshot for a region
    for the current chart week.

    Guarantees:
    - Valid region
    - Valid week
    - Idempotent (no overwrite)
    """
    if region not in VALID_REGIONS:
        raise ValueError(f"Invalid region: {region}")

    week_id = _get_week_id()
    path = _snapshot_path(region, week_id)

    # Idempotency guard (immutable snapshots)
    if path.exists():
        return json.loads(path.read_text())

    path.parent.mkdir(parents=True, exist_ok=True)

    items = _load_items_safe()

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

    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(payload, indent=2))
    tmp.replace(path)

    return payload


def load_region_snapshot(region: str) -> Optional[Dict]:
    """
    Load snapshot for current chart week.
    Returns None if missing or invalid.
    """
    if region not in VALID_REGIONS:
        return None

    try:
        week_id = _get_week_id()
    except RuntimeError:
        return None

    path = _snapshot_path(region, week_id)

    if not path.exists():
        return None

    try:
        return json.loads(path.read_text())
    except Exception:
        return None