from fastapi import APIRouter
from data.alerts import detect_missed_publish

router = APIRouter()

@router.get("/admin/health")
def health_check():
    alert = detect_missed_publish()
    return {
        "status": "ok",
        "alert": alert
    }