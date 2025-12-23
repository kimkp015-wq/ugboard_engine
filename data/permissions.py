# data/permissions.py

import os
from fastapi import HTTPException, Header


# Read tokens from environment (Railway-safe)
ADMIN_TOKEN = os.getenv("ADMIN_TOKEN")
INJECT_TOKEN = os.getenv("INJECT_TOKEN")


def _extract_token(auth_header: str | None) -> str | None:
    """
    Supports:
    - Authorization: Bearer TOKEN
    - Authorization: TOKEN
    """
    if not auth_header:
        return None

    value = auth_header.strip()

    if value.lower().startswith("bearer "):
        return value.split(" ", 1)[1].strip()

    return value


def ensure_admin_allowed(
    authorization: str | None = Header(default=None),
):
    token = _extract_token(authorization)

    if not ADMIN_TOKEN:
        raise HTTPException(
            status_code=503,
            detail="Admin token not configured",
        )

    if token != ADMIN_TOKEN:
        raise HTTPException(
            status_code=403,
            detail="Admin access denied",
        )


def ensure_injection_allowed(
    authorization: str | None = Header(default=None),
):
    # Lazy import to avoid startup crashes
    try:
        from data.region_store import any_region_locked
    except Exception:
        raise HTTPException(
            status_code=503,
            detail="Region lock system unavailable",
        )

    if any_region_locked():
        raise HTTPException(
            status_code=423,
            detail="Injection disabled after publish",
        )

    token = _extract_token(authorization)

    if not INJECT_TOKEN:
        raise HTTPException(
            status_code=503,
            detail="Injection token not configured",
        )

    if token != INJECT_TOKEN:
        raise HTTPException(
            status_code=403,
            detail="Injection access denied",
        )