# api/admin/internal.py

from fastapi import APIRouter, Depends
from data.permissions import ensure_internal_allowed

router = APIRouter()


@router.get(
    "/ping",
    summary="(Internal) Scheduler / system health check",
)
def internal_ping(
    _: None = Depends(ensure_internal_allowed),
):
    return {
        "status": "ok",
        "scope": "internal",
    }