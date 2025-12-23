from fastapi import APIRouter, Depends, HTTPException, Header
from typing import Optional

from api.admin.weekly_scheduler import run_weekly_scheduler
from data.chart_week import is_tracking_open

router = APIRouter()


# =========================
# Internal call protection
# =========================
def verify_internal_call(
    x_internal_token: Optional[str] = Header(default=None),
):
    """
    Prevent public access.

    This token MUST be injected via:
    - Cloudflare Scheduler
    - Railway cron
    """
    if not x_internal_token:
        raise HTTPException(
            status_code=401,
            detail="Missing internal token",
        )


# =========================
# Weekly automation runner
# =========================
@router.post(
    "/internal/weekly-run",
    summary="Run weekly publish / unlock automation (EAT)",
    tags=["Internal"],
)
def run_weekly(
    _: None = Depends(verify_internal_call),
):
    """
    Internal-only endpoint.

    Guarantees:
    - Safe to call multiple times
    - Will NOT publish during open tracking
    - Handles region publish + unlock
    """

    # Guardrail: never publish while tracking is open
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