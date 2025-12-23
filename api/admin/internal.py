from fastapi import APIRouter, Depends, HTTPException, Header

from api.admin.weekly_scheduler import run_weekly_scheduler
from data.chart_week import is_tracking_open
from data.alerts import detect_scheduler_alerts

router = APIRouter()


def verify_internal_call(
    x_internal_token: str | None = Header(default=None),
):
    if not x_internal_token:
        raise HTTPException(status_code=401, detail="Missing internal token")


@router.post(
    "/internal/weekly-run",
    summary="Run weekly scheduler (internal only)",
    tags=["Internal"],
)
def run_weekly(
    _: None = Depends(verify_internal_call),
):
    if is_tracking_open():
        return {
            "status": "skipped",
            "reason": "Tracking window still open",
        }

    result = run_weekly_scheduler()

    alert = detect_scheduler_alerts()

    return {
        "status": "ok",
        "result": result,
        "alert": alert,
    }