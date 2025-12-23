import os
from fastapi import HTTPException, Depends
from api.security import bearer_scheme
from data.region_store import any_region_locked

ADMIN_TOKEN = os.getenv("ADMIN_TOKEN")
INJECT_TOKEN = os.getenv("INJECT_TOKEN")


def ensure_admin_allowed(
    credentials = Depends(bearer_scheme),
):
    token = credentials.credentials

    if not ADMIN_TOKEN or token != ADMIN_TOKEN:
        raise HTTPException(
            status_code=403,
            detail="Admin access denied",
        )


def ensure_injection_allowed(
    credentials = Depends(bearer_scheme),
):
    if any_region_locked():
        raise HTTPException(
            status_code=423,
            detail="Injection disabled after publish",
        )

    token = credentials.credentials

    if not INJECT_TOKEN or token != INJECT_TOKEN:
        raise HTTPException(
            status_code=403,
            detail="Injection access denied",
        )