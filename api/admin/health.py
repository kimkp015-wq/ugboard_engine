# api/admin/health.py

from fastapi import APIRouter

router = APIRouter()


@router.get("/health")
def health():
    """
    Health must always respond.
    Alerts are OPTIONAL.
    """
    alert = None

    try:
        from data.alerts import detect_missed_publish
        alert = detect_missed_publish()
    except Exception:
        alert = "alerts_unavailable"

    return {
        "status": "ok",
        "engine": "ugboard",
        "alert": alert,
    }