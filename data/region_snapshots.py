# data/region_snapshots.py

import json
from pathlib import Path

SNAPSHOT_FILE = Path("data/region_snapshots.json")

DEFAULT = {
    "Eastern": [],
    "Northern": [],
    "Western": []
}

def load_region_snapshots():
    if not SNAPSHOT_FILE.exists():
        return DEFAULT.copy()

    try:
        return json.loads(SNAPSHOT_FILE.read_text())
    except Exception:
        return DEFAULT.copy()