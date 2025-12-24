# api/charts/top100.py

import json
from pathlib import Path
from typing import List, Dict

from fastapi import APIRouter

from data.chart_week import get_current_week_id

router = APIRouter()

# =========================
# Paths
# =========================

LOCKED_DIR = Path("data/top100_locked")
LIVE_FILE = Path("data/top100_live.json")

# =========================
# Internal helpers
# =========================

def _safe_read(path: Path):
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text())
    except Exception:
        return None


# =========================
# Public API (READ-ONLY)
# =========================

@router.get(
    "/top100",
    summary="Uganda Top 100 (current week)",
)
def get_top100() -> List[Dict]:
    """
    Read-only Uganda Top 100.

    Resolution order:
    1. Locked snapshot for current week (if published)
    2. Empty list (safe fallback)

    Never crashes.
    Never mutates state.
    """
    week_id = get_current_week_id()
    locked_file = LOCKED_DIR / f"{week_id}.json"

    locked = _safe_read(locked_file)
    if isinstance(locked, list):
        return locked

    return []


# =========================
# Explicit exports
# =========================

__all__ = ["router"]