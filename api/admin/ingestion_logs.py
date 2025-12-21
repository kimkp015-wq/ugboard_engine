from fastapi import APIRouter
from data.ingestion_log import read_logs

router = APIRouter()


@router.get("/admin/ingestion/logs")
def get_ingestion_logs():
    logs = read_logs()

    return {
        "status": "ok",
        "count": len(logs),
        "logs": logs
    }