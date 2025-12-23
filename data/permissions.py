import os
from fastapi import HTTPException, Security
from fastapi.security import APIKeyHeader
from data.region_store import any_region_locked

ADMIN_TOKEN = os.getenv("ADMIN_TOKEN")
INJECT_TOKEN = os.getenv("INJECT_TOKEN")

api_key_header = APIKeyHeader(
    name="Authorization",
    auto_error=False
)


def ensure_admin_allowed(
    api_key: str | None = Security(api_key_header),
):
    if not ADMIN_TOKEN or api_key != ADMIN_TOKEN:
        raise HTTPException(
            status_code=403,
            detail="Admin access denied",
        )


def ensure_injection_allowed(
    api_key: str | None = Security(api_key_header),
):
    if any_region_locked():
        raise HTTPException(
            status_code=423,
            detail="Injection disabled after publish",
        )

    if not INJECT_TOKEN or api_key != INJECT_TOKEN:
        raise HTTPException(
            status_code=403,
            detail="Injection access denied",
        )