import os
from fastapi import Header, HTTPException

INJECT_TOKEN = os.getenv("INJECT_TOKEN")
ADMIN_TOKEN = os.getenv("ADMIN_TOKEN")


def ensure_injection_allowed(
    authorization: str | None = Header(default=None),
):
    if not INJECT_TOKEN:
        raise HTTPException(
            status_code=500,
            detail="Injection token not configured",
        )

    if not authorization:
        raise HTTPException(
            status_code=403,
            detail="Injection access denied",
        )

    if authorization != f"Bearer {INJECT_TOKEN}":
        raise HTTPException(
            status_code=403,
            detail="Injection access denied",
        )


def ensure_admin_allowed(
    authorization: str | None = Header(default=None),
):
    if not ADMIN_TOKEN:
        raise HTTPException(
            status_code=500,
            detail="Admin token not configured",
        )

    if not authorization:
        raise HTTPException(
            status_code=403,
            detail="Admin access denied",
        )

    if authorization != f"Bearer {ADMIN_TOKEN}":
        raise HTTPException(
            status_code=403,
            detail="Admin access denied",
        )