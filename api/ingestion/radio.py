from fastapi import APIRouter, Depends
from data.permissions import ensure_injection_allowed

router = APIRouter()


@router.post("/radio", summary="Ingest Radio data")
def ingest_radio(
    payload: dict,
    _: None = Depends(ensure_injection_allowed),
):
    return {
        "status": "ok",
        "source": "radio",
        "received": True,
    }