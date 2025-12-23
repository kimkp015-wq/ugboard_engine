from fastapi.security import HTTPBearer

bearer_scheme = HTTPBearer(
    description="Paste: Bearer YOUR_TOKEN_HERE"
)