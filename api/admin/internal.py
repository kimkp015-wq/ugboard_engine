# api/admin/internal.py

from fastapi import APIRouter
from api.admin.weekly_scheduler import run_weekly_scheduler

router = APIRouter()


@router.post(
    "/internal/weekly-run",
    summary="Run weekly region publish/unlock automation (EAT)"
)
def run_weekly():
    """
    Internal-only endpoint.
    Safe to call multiple times.
    """
    return run_weekly_scheduler()