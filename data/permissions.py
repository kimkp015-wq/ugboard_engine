import os
from fastapi import Header, HTTPException
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials

# =========================
# Tokens from environment
# =========================

ADMIN_TOKEN = os.getenv("ADMIN_TOKEN")
INJECT_TOKEN = os.getenv("INJECT_TOKEN")
INTERNAL_TOKEN = os.getenv("INTERNAL_TOKEN")

bearer_scheme = HTTPBearer(auto_error=False)

# =========================
# Admin permission (Swagger + humans)
# Header: Authorization: Bearer TOKEN
# =========================

def ensure_admin_allowed(
    credentials: HTTPAuthorizationCredentials = Header(None),
):
    if not ADMIN_TOKEN:
        raise HTTPException(500, "ADMIN_TOKEN not configured")

    if not credentials or credentials.scheme.lower() != "bearer":
        raise HTTPException(403, "Admin authorization required")

    if credentials.credentials != ADMIN_TOKEN:
        raise HTTPException(403, "Admin access denied")

# =========================
# Ingestion permission
# Header: Authorization: Bearer TOKEN
# =========================

def ensure_injection_allowed(
    credentials: HTTPAuthorizationCredentials = Header(None),
):
    if not INJECT_TOKEN:
        raise HTTPException(500, "INJECT_TOKEN not configured")

    if not credentials or credentials.scheme.lower() != "bearer":
        raise HTTPException(403, "Injection authorization required")

    if credentials.credentials != INJECT_TOKEN:
        raise HTTPException(403, "Injection access denied")

# =========================
# Internal permission (Cloudflare cron)
# Header: X-Internal-Token
# =========================

def ensure_internal_allowed(
    x_internal_token: str = Header(None, alias="X-Internal-Token"),
):
    if not INTERNAL_TOKEN:
        raise HTTPException(500, "INTERNAL_TOKEN not configured")

    if x_internal_token != INTERNAL_TOKEN:
        raise HTTPException(403, "Internal access denied")
# Backward compatibility alias
ensure_ingest_allowed = ensure_injection_allowed

