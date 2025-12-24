# api/charts/top100.py

import json
from pathlib import Path
from typing import List, Dict

from fastapi import APIRouter

router = APIRouter()

# =========================
# Paths
# =========================

LOCKED_DIR = Path("data/top100_locked")
LIVE_FILE = Path("data/top100_live.json")

# =========================
# Public API (READ-ONLY)
# =========================

@router.get(
    "/top100",
    summary="Uganda Top 100 (current week)",
)
def get_top100() -> List[Dict]:
    """
    Read-only Top 100 chart.

    NOTE:
    - Will return locked data once week publishing is wired
    - Currently returns empty list safely
    """
    return []

# =========================
# Internal helpers
# =========================

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

# =========================
# Locking logic (ADMIN)
# =========================

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