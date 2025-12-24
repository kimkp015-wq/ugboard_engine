# api/charts/index.py

from fastapi import APIRouter
from data.index import get_index

router = APIRouter()


@router.get(
    "/index",
    summary="Get published chart history (immutable index)",
)
def get_chart_index():
    """
    Public, read-only endpoint.

    Returns:
    - All published chart weeks
    - Source of truth for chart releases
    """

    index = get_index()

    return {
        "status": "ok",
        "count": len(index),
        "weeks": index,
    }