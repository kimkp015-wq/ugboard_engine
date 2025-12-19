from fastapi import APIRouter, Header, HTTPException, Depends
from datetime import datetime

router = APIRouter(prefix="/admin", tags=["Admin"])


# ─────────────────────────────────────────────
# Admin key protection
# ─────────────────────────────────────────────
def require_admin_key(x_admin_key: str = Header(None)):
    if x_admin_key != "SECRET_ADMIN_KEY":
        raise HTTPException(status_code=401, detail="Unauthorized")


# ─────────────────────────────────────────────
# Publish Top 100 Chart
# ─────────────────────────────────────────────
@router.post(
    "/publish/top100",
    dependencies=[Depends(require_admin_key)]
)
def publish_top100():
    """
    Publishes Top 100 chart
    (stub logic – safe to extend later)
    """
    return {
        "status": "success",
        "chart": "top100",
        "published_at": datetime.utcnow().isoformat()
    }