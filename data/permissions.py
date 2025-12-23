import os
from fastapi import Header, HTTPException

# =========================
# Tokens from environment
# =========================

ADMIN_TOKEN = os.getenv("ADMIN_TOKEN")
INJECT_TOKEN = os.getenv("INJECT_TOKEN")
INTERNAL_TOKEN = os.getenv("INTERNAL_TOKEN")

# =========================
# Admin permission
# =========================

def ensure_admin_allowed(
    x_admin_token: str = Header(None),
):
    if not ADMIN_TOKEN:
        raise HTTPException(500, "ADMIN_TOKEN not configured")

    if x_admin_token != ADMIN_TOKEN:
        raise HTTPException(status_code=403, detail="Access denied")

# =========================
# Ingestion permission
# =========================

def ensure_injection_allowed(
    x_inject_token: str = Header(None),
):
    if not INJECT_TOKEN:
        raise HTTPException(500, "INJECT_TOKEN not configured")

    if x_inject_token != INJECT_TOKEN:
        raise HTTPException(status_code=403, detail="Injection access denied")

# =========================
# Internal (cron / system)
# =========================

def ensure_internal_allowed(
    x_internal_token: str = Header(None),
):
    if not INTERNAL_TOKEN:
        raise HTTPException(500, "INTERNAL_TOKEN not configured")

    if x_internal_token != INTERNAL_TOKEN:
        raise HTTPException(status_code=403, detail="Access denied")