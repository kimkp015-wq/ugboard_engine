import json
from pathlib import Path
from datetime import datetime

SNAPSHOT_DIR = Path("data/region_snapshots")
SNAPSHOT_DIR.mkdir(exist_ok=True)

def snapshot_path(region: str) -> Path:
    return SNAPSHOT_DIR / f"{region.lower()}.json"

def save_region_snapshot(region: str, songs: list):
    payload = {
        "region": region,
        "published_at": datetime.utcnow().isoformat(),
        "songs": songs
    }
    snapshot_path(region).write_text(json.dumps(payload, indent=2))

def load_region_snapshot(region: str):
    path = snapshot_path(region)
    if not path.exists():
        return None
    return json.loads(path.read_text())