# api/admin/alerts.py

from fastapi import APIRouter, Depends

router = APIRouter()


def _admin_guard():
    """
    Lazy-load admin permission check.
    Prevents startup crashes hiding Swagger routes.
    """
    try:
        from data.permissions import ensure_admin_allowed
        return ensure_admin_allowed
    except Exception:
        # Fail CLOSED but do not crash startup
        def deny():
            from fastapi import HTTPException
            raise HTTPException(status_code=503, detail="Admin system unavailable")

        return deny


@router.get(
    "/alerts",
    summary="Engine alert status (admin only)",
)
def get_alerts(
    _: None = Depends(_admin_guard()),
):
    """
    Read-only alert endpoint.
    Never mutates state.
    Safe to call anytime.
    """
    from data.alerts import collect_alerts
    from data.scheduler_state import get_last_scheduler_run

    last_run = get_last_scheduler_run()
    return collect_alerts(last_run)