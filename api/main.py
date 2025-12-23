import os
from fastapi import FastAPI

ENV = os.getenv("ENV", "development")

app = FastAPI(
    title="UG Board Engine",
    docs_url="/docs" if ENV != "production" else None,
    redoc_url="/redoc" if ENV != "production" else None,
)

@app.get("/", tags=["Health"])
def root():
    return {
        "engine": "UG Board Engine",
        "status": "online",
        "environment": ENV,
        "docs_enabled": ENV != "production",
    }
    # AFTER app is created
from api.admin.health import router as health_router

app.include_router(health_router, tags=["Health"])