# api/admin/internal.py

from fastapi import APIRouter, Depends

from data.permissions import ensure_internal_allowed
from data.chart_week import current_chart_week
from data.scheduler_state import get_last_scheduler_run
from data.region_store import get_region_state

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


@router.get(
    "/state",
    summary="(Internal) Engine state snapshot (read-only)",
)
def internal_state(
    _: None = Depends(ensure_internal_allowed),
):
    """
    Read-only engine state.

    Used by:
    - Cloudflare Workers (verification)
    - Debugging / audits

    Never mutates data.
    """

    return {
        "chart_week": current_chart_week(),
        "last_scheduler_run": get_last_scheduler_run(),
        "regions": {
            "Eastern": get_region_state("Eastern"),
            "Northern": get_region_state("Northern"),
            "Western": get_region_state("Western"),
        },
    }