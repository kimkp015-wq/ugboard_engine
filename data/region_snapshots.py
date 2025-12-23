import json
from pathlib import Path
from data.store import load_items

SNAPSHOT_DIR = Path("data/region_snapshots")


def save_region_snapshot(region: str) -> None:
    SNAPSHOT_DIR.mkdir(parents=True, exist_ok=True)

    items = load_items()
    region_items = [i for i in items if i.get("region") == region]

    region_items.sort(
        key=lambda x: x.get("score", 0),
        reverse=True
    )

    snapshot = region_items[:5]

    path = SNAPSHOT_DIR / f"{region.lower()}.json"
    path.write_text(json.dumps(snapshot, indent=2))


def load_region_snapshot(region: str):
    path = SNAPSHOT_DIR / f"{region.lower()}.json"
    if not path.exists():
        return None
    return json.loads(path.read_text())