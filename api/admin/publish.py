from fastapi import APIRouter, HTTPException
from datetime import datetime

router = APIRouter(
    prefix="/admin",
    tags=["admin"]
)

@router.post("/publish/top100")
def publish_top100():
    """
    Publish Top 100 chart.
    (DB logic will be added later)
    """
    return {
        "status": "success",
        "chart": "top100",
        "published_at": datetime.utcnow().isoformat()
    }


@router.get("/health")
def admin_health():
    return {"status": "admin ok"}