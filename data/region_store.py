from pathlib import Path
from datetime import datetime
from zoneinfo import ZoneInfo
import json

STORE = Path("data/regions.json")
EAT = ZoneInfo("Africa/Kampala")


def _load():
    if not STORE.exists():
        return {}
    return json.loads(STORE.read_text())


def _save(data):
    STORE.write_text(json.dumps(data, indent=2))


def is_frozen(region: str, week: str) -> bool:
    data = _load()
    return region in data and data[region]["week"] == week


def publish_region(region: str, items: list, week: str):
    data = _load()
    data[region] = {
        "week": week,
        "published_at": datetime.now(EAT).isoformat(),
        "items": items
    }
    _save(data)


def get_region(region: str):
    return _load().get(region)