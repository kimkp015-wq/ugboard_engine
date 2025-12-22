import json
from pathlib import Path
from typing import List, Dict

SNAPSHOT_DIR = Path("data/region_snapshots")
SNAPSHOT_DIR.mkdir(parents=True, exist_ok=True)


def _snapshot_path(region: str) -> Path:
    return SNAPSHOT_DIR / f"{region.lower()}.json"


def save_region_snapshot(region: str, songs: List[Dict]):
    """
    Save Top 5 snapshot for a region.
    This is called ONLY when publishing.
    """
    path = _snapshot_path(region)
    path.write_text(json.dumps(songs, indent=2))


def load_region_snapshot(region: str):
    """
    Load published snapshot for a region.
    Returns None if not published.
    """
    path = _snapshot_path(region)
    if not path.exists():
        return None

    try:
        return json.loads(path.read_text())
    except Exception:
        return None