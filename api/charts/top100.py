import json
from pathlib import Path
from typing import List, Dict

LOCKED_DIR = Path("data/top100_locked")
LIVE_FILE = Path("data/top100_live.json")


def _safe_read(path: Path):
    try:
        return json.loads(path.read_text())
    except Exception:
        return None


def _safe_write(path: Path, data) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(data, indent=2))
    tmp.replace(path)


def lock_top100(week_id: str) -> None:
    """
    Lock current Top 100 for a given week.

    Guarantees:
    - Idempotent (will not overwrite)
    - Atomic write
    - Immutable once created
    """
    LOCKED_DIR.mkdir(parents=True, exist_ok=True)
    target = LOCKED_DIR / f"{week_id}.json"

    # Idempotency guard
    if target.exists():
        return

    live = _safe_read(LIVE_FILE)

    if not isinstance(live, list):
        raise RuntimeError("Top100 live file is missing or invalid")

    _safe_write(target, live)