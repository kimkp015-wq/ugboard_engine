import os
from fastapi import Header, HTTPException

ADMIN_TOKEN = os.getenv("ADMIN_TOKEN")
INJECT_TOKEN = os.getenv("INJECT_TOKEN")


def ensure_admin_allowed(
    authorization: str | None = Header(default=None),
):
    if not ADMIN_TOKEN:
        raise HTTPException(
            status_code=500,
            detail="ADMIN_TOKEN not configured",
        )

    if authorization != ADMIN_TOKEN:
        raise HTTPException(
            status_code=403,
            detail="Admin access denied",
        )


def ensure_injection_allowed(
    authorization: str | None = Header(default=None),
):
    if not INJECT_TOKEN:
        raise HTTPException(
            status_code=500,
            detail="INJECT_TOKEN not configured",
        )

    if authorization != INJECT_TOKEN:
        raise HTTPException(
            status_code=403,
            detail="Injection access denied",
        )
 