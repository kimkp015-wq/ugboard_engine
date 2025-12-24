import json
from pathlib import Path
from typing import List, Dict

from data.store import load_items
from data.chart_week import current_chart_week

TOP100_DIR = Path("data/top100_snapshots")


def save_top100_snapshot() -> Dict:
    week = current_chart_week()
    week_id = week["week_id"]

    path = TOP100_DIR / f"{week_id}.json"
    path.parent.mkdir(parents=True, exist_ok=True)

    items = load_items()
    items.sort(key=lambda x: x.get("score", 0), reverse=True)

    snapshot = items[:100]

    payload = {
        "week_id": week_id,
        "count": len(snapshot),
        "items": snapshot,
    }

    path.write_text(json.dumps(payload, indent=2))
    return payload


def load_top100_snapshot():
    week_id = current_chart_week()["week_id"]
    path = TOP100_DIR / f"{week_id}.json"

    if not path.exists():
        return None

    return json.loads(path.read_text())