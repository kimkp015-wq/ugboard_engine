import os
from fastapi import Header, HTTPException

# =========================
# Tokens from environment
# =========================

ADMIN_TOKEN = os.getenv("ADMIN_TOKEN")
INJECT_TOKEN = os.getenv("INJECT_TOKEN")
X_INTERNAL_TOKEN = os.getenv("X_INTERNAL_TOKEN")

# =========================
# Admin permission (Swagger / humans)
# Header: Authorization
# =========================

def ensure_admin_allowed(
    authorization: str = Header(None),
):
    if not ADMIN_TOKEN:
        raise HTTPException(status_code=500, detail="ADMIN_TOKEN not configured")

    if authorization != ADMIN_TOKEN:
        raise HTTPException(status_code=403, detail="Admin access denied")

# =========================
# Ingestion permission (YouTube / Radio / TV)
# Header: Authorization
# =========================

def ensure_injection_allowed(
    authorization: str = Header(None),
):
    if not INJECT_TOKEN:
        raise HTTPException(status_code=500, detail="INJECT_TOKEN not configured")

    if authorization != INJECT_TOKEN:
        raise HTTPException(status_code=403, detail="Injection access denied")

# =========================
# Internal permission (Cloudflare cron)
# Header: X-Internal-Token
# =========================

def ensure_internal_allowed(
    x_internal_token: str = Header(None, alias="X-Internal-Token"),
):
    if not X_INTERNAL_TOKEN:
        raise HTTPException(status_code=500, detail="X_INTERNAL_TOKEN not configured")

    if x_internal_token != X_INTERNAL_TOKEN:
        raise HTTPException(status_code=403, detail="Internal access denied")