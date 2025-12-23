# api/admin/alerts.py

from fastapi import APIRouter, Depends
from data.permissions import ensure_admin_allowed

router = APIRouter()


@router.get(
    "/alerts",
    summary="Admin alerts (missed or failed publishes)",
    tags=["Admin"],
)
def admin_alerts(_: None = Depends(ensure_admin_allowed)):
    """
    Returns engine alerts.
    SAFE: never crashes startup.
    """
    from data.alerts import detect_missed_publish

    alert = detect_missed_publish()

    return {
        "status": "ok",
        "alert": alert,
    }