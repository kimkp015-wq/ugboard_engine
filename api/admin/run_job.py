# In your FastAPI app (likely main.py or similar)
from fastapi import FastAPI, Header, HTTPException

app = FastAPI()

# This should be in your environment variables on Railway too!
MANUAL_TRIGGER_TOKEN = "test123"  # Or load from env var

@app.post("/admin/run-job")
async def admin_run_job(
    x_manual_trigger: str = Header(..., alias="X-Manual-Trigger")
):
    """Endpoint for manual triggers from Cloudflare Worker"""
    
    # Validate the manual trigger token
    if x_manual_trigger != MANUAL_TRIGGER_TOKEN:
        raise HTTPException(
            status_code=403,
            detail="Invalid manual trigger token"
        )
    
    # Your existing ingestion logic here
    return {"message": "Manual trigger executed", "status": "success"}
