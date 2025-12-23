# api/admin/internal.py

from fastapi import APIRouter, Depends, HTTPException, Header
from api.admin.weekly_scheduler import run_weekly_scheduler
from data.chart_week import is_tracking_open

router = APIRouter()


def verify_internal_call(
    x_internal_token: str | None = Header(default=None),
):
    """
    Prevent public access.
    Token should be injected via Cloudflare / Railway secret.
    """
    if not x_internal_token:
        raise HTTPException(status_code=401, detail="Missing internal token")


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

    # Guardrail: do NOT allow weekly publish during open tracking
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