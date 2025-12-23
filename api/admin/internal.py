# api/admin/internal.py

from fastapi import APIRouter, Depends, HTTPException, Header

from api.admin.weekly_scheduler import run_weekly_scheduler
from data.chart_week import is_tracking_open
from data.alerts import detect_internal_scheduler_alert

router = APIRouter()


def verify_internal_call(
    x_internal_token: str | None = Header(default=None),
):
    """
    Prevent public access.
    Token must be injected via Cloudflare / Railway secret.
    """
    if not x_internal_token:
        raise HTTPException(status_code=401, detail="Missing internal token")


@router.get(
    "/internal/health",
    summary="Internal scheduler health check",
    tags=["Internal"],
)
def internal_health(
    _: None = Depends(verify_internal_call),
):
    return {"status": "ok"}


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
    Safe to call multiple times.
    """

    if is_tracking_open():
        return {
            "status": "skipped",
            "reason": "Tracking window still open",
        }

    try:
        result = run_weekly_scheduler()
    except Exception as e:
        detect_internal_scheduler_alert(
            event="weekly_run_failed",
            detail=str(e),
        )
        raise

    return {
        "status": "ok",
        "result": result,
    }