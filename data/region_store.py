import json
from pathlib import Path
from datetime import datetime

REGION_LOCKS_FILE = Path("data/region_locks.json")

DEFAULT_LOCKS = {
    "Eastern": False,
    "Northern": False,
    "Western": False,
    "last_updated": None,
}


def load_region_locks() -> dict:
    if not REGION_LOCKS_FILE.exists():
        save_region_locks(DEFAULT_LOCKS)
        return DEFAULT_LOCKS.copy()

    try:
        return json.loads(REGION_LOCKS_FILE.read_text())
    except Exception:
        return DEFAULT_LOCKS.copy()


def save_region_locks(data: dict) -> None:
    REGION_LOCKS_FILE.parent.mkdir(parents=True, exist_ok=True)
    REGION_LOCKS_FILE.write_text(json.dumps(data, indent=2))


def is_region_locked(region: str) -> bool:
    locks = load_region_locks()
    return bool(locks.get(region, False))


def lock_region(region: str) -> None:
    locks = load_region_locks()
    locks[region] = True
    locks["last_updated"] = datetime.utcnow().isoformat()
    save_region_locks(locks)


def unlock_region(region: str) -> None:
    locks = load_region_locks()
    locks[region] = False
    locks["last_updated"] = datetime.utcnow().isoformat()
    save_region_locks(locks)

def publish_region(region: str):
    """
    Publish = lock region.
    """
    lock_region(region)

def any_region_locked() -> bool:
    locks = load_region_locks()

    for key, value in locks.items():
        if key == "last_updated":
            continue
        if value is True:
            return True

    return False