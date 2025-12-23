import os
from fastapi import Header, HTTPException

INJECTION_TOKEN = os.getenv("INJECTION_TOKEN")
ADMIN_TOKEN = os.getenv("ADMIN_TOKEN")


def ensure_injection_allowed(
    authorization: str = Header(
        None,
        description="Paste INJECTION token here (inject-ug-board-2025)"
    )
):
    if not INJECTION_TOKEN:
        raise HTTPException(500, "INJECTION_TOKEN not set on server")

    if authorization != INJECTION_TOKEN:
        raise HTTPException(403, "Injection access denied")


def ensure_admin_allowed(
    authorization: str = Header(
        None,
        description="Paste ADMIN token here (admin-ug-board-2025)"
    )
):
    if not ADMIN_TOKEN:
        raise HTTPException(500, "ADMIN_TOKEN not set on server")

    if authorization != ADMIN_TOKEN:
        raise HTTPException(403, "Admin access denied")