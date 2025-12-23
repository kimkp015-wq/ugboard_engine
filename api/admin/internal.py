# api/admin/internal.py

from fastapi import APIRouter, Depends, HTTPException, Header, Request
from datetime import datetime
from api.admin.weekly_scheduler import run_weekly_scheduler
from data.chart_week import is_tracking_open
from data.audit import log_audit
import os

router = APIRouter()

INTERNAL_TOKEN = os.getenv("X_INTERNAL_TOKEN")


def verify_internal_call(
    request: Request,
    x_internal_token: str | None = Header(default=None),
):
    """
    Verify internal scheduler / automation calls.
    Logs failed attempts for security visibility.
    """

    if not x_internal_token or x_internal_token != INTERNAL_TOKEN:
        log_audit(
            action="internal_auth_failed",
            path=str(request.url.path),
            ip=request.client.host if request.client else "unknown",
            timestamp=datetime.utcnow().isoformat(),
        )

        raise HTTPException(
            status_code=401,
            detail="Unauthorized internal request",
        )


@router.post(
    "/internal/weekly-run",
    summary="Run weekly region publish/unlock automation (EAT)",
    tags=["Internal"],
)
def run_weekly(
    _: None = Depends(verify_internal_call),
):
    """
    Internal-only endpoint.
    - Safe to call multiple times
    - Publishes charts
    - Locks admin injection
    - Unlocks next tracking window
    """

    if is_tracking_open():
        return {
            "status": "skipped",
            "reason": "Tracking window still open",
        }

    result = run_weekly_scheduler()

    return {
        "status": "ok",
        "result": result,
    }
    @router.get(
    "/internal/health",
    summary="Internal scheduler authentication check",
    tags=["Internal"],
)
def internal_health(
    _: None = Depends(verify_internal_call),
):
    return {
        "status": "ok",
        "service": "ugboard-api",
        "auth": "valid",
    }