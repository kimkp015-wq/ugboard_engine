# data/region_snapshots.py

import json
from pathlib import Path
from datetime import datetime

SNAPSHOT_FILE = Path("data/region_snapshots.json")


def _load_all():
    if not SNAPSHOT_FILE.exists():
        return {}
    try:
        return json.loads(SNAPSHOT_FILE.read_text())
    except Exception:
        return {}


def _save_all(data: dict):
    SNAPSHOT_FILE.parent.mkdir(parents=True, exist_ok=True)
    SNAPSHOT_FILE.write_text(json.dumps(data, indent=2))


def save_region_snapshot(region: str, items: list):
    data = _load_all()
    data[region] = {
        "region": region,
        "published_at": datetime.utcnow().isoformat(),
        "items": items
    }
    _save_all(data)


def load_region_snapshot(region: str):
    data = _load_all()
    snapshot = data.get(region)
    if not snapshot:
        return None
    return snapshot.get("items", [])