# api/admin/alerts.py

from fastapi import APIRouter

router = APIRouter()


@router.get(
    "/alerts",
    summary="Admin alerts (missed publish, partial failures)",
)
def get_alerts():
    """
    Admin-facing alerts.
    SAFE: never crashes the app.
    """
    try:
        from data.alerts import detect_missed_publish
        alert = detect_missed_publish()
    except Exception as e:
        return {
            "status": "degraded",
            "error": str(e),
        }

    return {
        "status": "ok",
        "alert": alert,
    }