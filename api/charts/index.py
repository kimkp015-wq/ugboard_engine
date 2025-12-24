# api/charts/index.py

from fastapi import APIRouter
from typing import List, Dict

from data.index import get_index

router = APIRouter()

@router.get(
    "/index",
    summary="Public chart publish index",
)
def read_index() -> List[Dict]:
    """
    Read-only public index of published chart weeks.

    Source of truth:
    - data/index.json (append-only)
    """
    return get_index()