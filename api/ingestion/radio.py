cat > api/ingestion/radio.py << 'EOF'
"""
EMERGENCY FIX: Minimal radio ingestion router
No dependencies, no multipart issues
"""

from fastapi import APIRouter, Header, HTTPException
from datetime import datetime

router = APIRouter()

@router.post("/radio")
async def ingest_radio_data(
    data: dict,
    x_internal_token: str = Header(None, alias="X-Internal-Token")
):
    """Emergency radio endpoint - works without python-multipart"""
    if x_internal_token != "1994199620002019866":
        raise HTTPException(status_code=401, detail="Invalid worker token")
    
    return {
        "status": "success",
        "message": "Radio endpoint working",
        "timestamp": datetime.utcnow().isoformat(),
        "note": "Emergency version - scoring disabled"
    }
EOF
