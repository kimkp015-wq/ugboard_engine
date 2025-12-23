# api/admin/alerts.py

from fastapi import APIRouter, Depends
from data.alerts import detect_publish_alert
from data.permissions import ensure_admin_allowed

router = APIRouter()


@router.get(
    "/alerts",
    summary="Check for failed or partial weekly publish",
)
def check_alerts(
    _: None = Depends(ensure_admin_allowed),
):
    alert = detect_publish_alert()

    if not alert:
        return {
            "status": "ok",
            "message": "No alerts",
        }

    return {
        "status": "alert",
        "alert": alert,
    }