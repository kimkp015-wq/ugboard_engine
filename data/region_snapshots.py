# data/region_snapshots.py

import json
from pathlib import Path
from datetime import datetime
from zoneinfo import ZoneInfo
from typing import List, Dict

from data.store import load_items

EAT = ZoneInfo("Africa/Kampala")

SNAPSHOT_DIR = Path("data/region_snapshots")
VALID_REGIONS = ("Eastern", "Northern", "Western")


def _now() -> str:
    return datetime.now(EAT).isoformat()


def _snapshot_path(region: str) -> Path:
    return SNAPSHOT_DIR / f"{region.lower()}.json"


def save_region_snapshot(region: str) -> Dict:
    """
    Generate and persist Top 5 snapshot for a region.
    Atomic and auditable.
    """
    if region not in VALID_REGIONS:
        raise ValueError("Invalid region")

    SNAPSHOT_DIR.mkdir(parents=True, exist_ok=True)

    items = load_items() or []

    region_items: List[Dict] = [
        i for i in items if i.get("region") == region
    ]

    region_items.sort(
        key=lambda x: x.get("score", 0),
        reverse=True,
    )

    top_items = region_items[:5]

    snapshot = {
        "region": region,
        "generated_at": _now(),
        "count": len(top_items),
        "items": top_items,
    }

    # Atomic write
    tmp_path = _snapshot_path(region).with_suffix(".json.tmp")
    final_path = _snapshot_path(region)

    tmp_path.write_text(json.dumps(snapshot, indent=2))
    tmp_path.replace(final_path)

    return snapshot


def load_region_snapshot(region: str) -> Dict | None:
    """
    Load persisted region snapshot.
    Safe read.
    """
    if region not in VALID_REGIONS:
        return None

    path = _snapshot_path(region)
    if not path.exists():
        return None

    try:
        return json.loads(path.read_text())
    except Exception:
        return None