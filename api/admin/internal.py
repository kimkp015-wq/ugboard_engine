# api/admin/internal.py

from fastapi import APIRouter

router = APIRouter()


@router.get("/internal/ping")
def internal_ping():
    return {"status": "internal ok"}