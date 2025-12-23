# api/admin/internal.py

import os
from fastapi import APIRouter, Depends, HTTPException, Header
from api.admin.weekly_scheduler import run_weekly_scheduler
from data.chart_week import is_tracking_open

router = APIRouter()

INTERNAL_TOKEN = os.getenv("INTERNAL_TOKEN")


def verify_internal_call(
    x_internal_token: str | None = Header(default=None),
):
    """
    Prevent public access.
    Token must match INTERNAL_TOKEN env variable.
    """
    if not INTERNAL_TOKEN or x_internal_token != INTERNAL_TOKEN:
        raise HTTPException(
            status_code=401,
            detail="Invalid or missing internal token",
        )


@router.post(
    "/internal/weekly-run",
    summary="Run weekly publish & lock automation",
)
def run_weekly(
    _: None = Depends(verify_internal_call),
):
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