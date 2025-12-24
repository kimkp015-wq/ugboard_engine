# api/charts/index.py

from fastapi import APIRouter
from typing import List, Dict

from data.index import get_index

router = APIRouter()

# =========================
# Public chart index
# =========================

@router.get(
    "/index",
    summary="Public chart publish index",
    tags=["Charts"],
)
def read_index() -> Dict:
    """
    Read-only public index of published chart weeks.

    Source of truth:
    - data/index.json (append-only)
    """
    entries: List[Dict] = get_index()

    return {
        "status": "ok",
        "count": len(entries),
        "items": entries,
    }