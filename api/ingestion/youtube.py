from fastapi import APIRouter, Depends
from data.permissions import ensure_injection_allowed

router = APIRouter()


@router.post("/youtube", summary="Ingest YouTube data")
def ingest_youtube(
    payload: dict,
    _: None = Depends(ensure_injection_allowed),
):
    # Stub -- logic comes later
    return {
        "status": "ok",
        "source": "youtube",
        "received": True,
    }