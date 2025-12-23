from fastapi import APIRouter, Depends
from data.permissions import ensure_injection_allowed

router = APIRouter()


@router.post("/tv", summary="Ingest TV data")
def ingest_tv(
    payload: dict,
    _: None = Depends(ensure_injection_allowed),
):
    return {
        "status": "ok",
        "source": "tv",
        "received": True,
    }