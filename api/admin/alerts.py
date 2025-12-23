# api/admin/alerts.py

from fastapi import APIRouter, Depends
from data.alerts import detect_publish_alert
from data.permissions import ensure_admin_allowed

router = APIRouter()


@router.get(
    "/alerts/publish",
    summary="Check weekly publish status alerts",
    tags=["Admin"],
)
def publish_alert(
    _: None = Depends(ensure_admin_allowed)
):
    """
    Returns structured alert data if publish missing or partial.
    Otherwise returns ok with no alert.
    """
    alert = detect_publish_alert()

    return {
        "status": "ok",
        "alert": alert,
    }