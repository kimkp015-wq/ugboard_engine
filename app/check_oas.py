# app/check_oas.py
from fastapi import FastAPI
import json

app = FastAPI()

@app.get("/openapi.json")
async def get_openapi():
    """Check your current OpenAPI specification"""
    openapi_schema = app.openapi()
    
    # Check version
    openapi_version = openapi_schema.get("openapi", "Unknown")
    print(f"Current OpenAPI version: {openapi_version}")
    
    # Check components
    has_webhooks = "webhooks" in openapi_schema
    print(f"Has webhooks support: {has_webhooks}")
    
    return openapi_schema

@app.get("/api-info")
async def api_info():
    """Get API information including OAS version"""
    openapi_schema = app.openapi()
    return {
        "openapi_version": openapi_schema.get("openapi"),
        "title": openapi_schema.get("info", {}).get("title"),
        "version": openapi_schema.get("info", {}).get("version"),
        "paths_count": len(openapi_schema.get("paths", {})),
        "has_components": "components" in openapi_schema,
        "has_webhooks": "webhooks" in openapi_schema
    }
