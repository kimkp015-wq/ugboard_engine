# api/charts/trending.py

import json
from pathlib import Path
from fastapi import APIRouter

from data.chart_week import get_current_week_id

router = APIRouter()

# =========================
# Paths
# =========================

TRENDING_FILE = Path("data/trending.json")


# =========================
# Internal helpers
# =========================

def _safe_read_trending():
    """
    Safe read for trending data.
    Never raises, never crashes runtime.
    """
    if not TRENDING_FILE.exists():
        return []

    try:
        data = json.loads(TRENDING_FILE.read_text())
        if isinstance(data, dict):
            items = data.get("items", [])
            return items if isinstance(items, list) else []
        return []
    except Exception:
        return []


# =========================
# Public API (READ-ONLY)
# =========================

@router.get(
    "/trending",
    summary="Trending songs (live)",
)
def get_trending():
    """
    Live trending chart.
    Safe, read-only, week-aware.
    """
    week_id = get_current_week_id()
    items = _safe_read_trending()

    return {
        "status": "ok",
        "week_id": week_id,
        "count": len(items),
        "items": items,
    }