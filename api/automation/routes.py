from fastapi import APIRouter, HTTPException, Header
from api.automation.weekly_regions import run_weekly_region_publish

router = APIRouter()

# Simple protection (can be replaced later)
AUTOMATION_KEY = "weekly-region-secret"


@router.post("/automation/weekly/regions")
def trigger_weekly_regions(
    x_automation_key: str = Header(None)
):
    """
    Safe endpoint triggered by scheduler.
    """
    if x_automation_key != AUTOMATION_KEY:
        raise HTTPException(
            status_code=403,
            detail="Invalid automation key"
        )

    result = run_weekly_region_publish()
    return result