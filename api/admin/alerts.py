# api/admin/alerts.py

from fastapi import APIRouter, Depends

from data.alerts import collect_alerts
from data.permissions import ensure_admin_allowed
from data.scheduler_state import get_last_scheduler_run

router = APIRouter()


@router.get(
    "/alerts",
    summary="Engine alert status (admin only)",
)
def get_alerts(
    _: None = Depends(ensure_admin_allowed),
):
    """
    Read-only alert endpoint.
    Never mutates state.
    Safe to call anytime.
    """
    last_run = get_last_scheduler_run()
    return collect_alerts(last_run)