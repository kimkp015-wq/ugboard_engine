# data/permissions.py

from fastapi import HTTPException, Header
from data.region_store import any_region_locked

ADMIN_TOKEN = "ENGINE_SECRET_TOKEN"
INJECT_TOKEN = "ENGINE_INJECT_TOKEN"


def ensure_admin_allowed(
    authorization: str = Header(None)
):
    if authorization != ADMIN_TOKEN:
        raise HTTPException(
            status_code=403,
            detail="Admin access denied"
        )


def ensure_injection_allowed(
    authorization: str = Header(None)
):
    if any_region_locked():
        raise HTTPException(
            status_code=423,
            detail="Injection disabled after publish"
        )

    if authorization != INJECT_TOKEN:
        raise HTTPException(
            status_code=403,
            detail="Injection access denied"
        )