# data/alerts.py

from typing import Optional, Dict
from data.region_store import load_region_locks
from data.region_snapshots import load_region_snapshot

REGIONS = ["Eastern", "Northern", "Western"]


def detect_publish_alert() -> Optional[Dict]:
    """
    Detect if weekly publish was missed or partial.

    Returns alert dictionary with details or None if publish is complete.
    """

    locks = load_region_locks()
    locked_status = {r: bool(locks.get(r, False)) for r in REGIONS}

    # If none locked => publish missing
    if not any(locked_status.values()):
        return {
            "type": "MISSING_PUBLISH",
            "message": "No regions published this week",
            "locks": locked_status
        }

    # If some locked but not all => partial publish
    if not all(locked_status.values()):
        missing = [r for r, l in locked_status.items() if not l]
        return {
            "type": "PARTIAL_PUBLISH",
            "message": "Partial publish detected",
            "locked": [r for r, l in locked_status.items() if l],
            "missing": missing,
        }

    # All locked -- check snapshot validity
    snapshot_errors = []
    for region in REGIONS:
        snapshot = load_region_snapshot(region)
        if snapshot is None:
            snapshot_errors.append(region)

    if snapshot_errors:
        return {
            "type": "SNAPSHOT_MISSING",
            "message": "One or more region snapshots missing despite lock",
            "regions_missing_snapshots": snapshot_errors,
        }

    # Everything published, no alert
    return None