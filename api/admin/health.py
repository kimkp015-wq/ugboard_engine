# api/admin/health.py

from fastapi import APIRouter

router = APIRouter()

# =========================
# Health endpoint (ADMIN)
# =========================

@router.get(
    "/health",
    summary="Admin health check",
    tags=["Health"],
)
def health():
    """
    Admin health endpoint.

    Guarantees:
    - Never crashes
    - Never blocks startup
    - Alerts are optional and best-effort
    """
    alert = None

    try:
        # Lazy + isolated import
        from data.alerts import detect_missed_publish

        try:
            alert = detect_missed_publish()
        except Exception:
            alert = "alert_check_failed"

    except Exception:
        alert = "alerts_unavailable"

    return {
        "status": "ok",
        "engine": "UG Board Engine",
        "alert": alert,
    }