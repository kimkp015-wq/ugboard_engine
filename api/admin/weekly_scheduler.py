from fastapi import APIRouter, HTTPException, Header
from api.admin.weekly_scheduler import run_weekly_scheduler
from data.chart_week import is_tracking_open

router = APIRouter()


def verify_internal_call(x_internal_token: str | None = Header(default=None)):
    if not x_internal_token:
        raise HTTPException(status_code=401, detail="Missing internal token")


@router.post("/internal/weekly-run")
def run_weekly(_: None = Header(default=None)):
    if is_tracking_open():
        return {
            "status": "skipped",
            "reason": "Tracking window still open"
        }

    result = run_weekly_scheduler()

    return {
        "status": "ok",
        "result": result
    }