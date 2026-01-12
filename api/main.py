# api/main.py - Add to top
from fastapi import FastAPI
from fastapi.openapi.utils import get_openapi

def custom_openapi():
    if app.openapi_schema:
        return app.openapi_schema
    
    openapi_schema = get_openapi(
        title="UG Board Engine API",
        version="1.0.0",
        description="""
        # UG Board Music Charting Engine
        
        ## Overview
        Automated music chart system aggregating data from:
        - YouTube videos
        - Radio play data  
        - TV broadcast data
        
        ## Authentication
        1. **Internal/Cloudflare**: `X-Internal-Token` header
        2. **Ingestion Clients**: `Authorization: Bearer <INJECT_TOKEN>`
        3. **Admin Access**: `Authorization: Bearer <ADMIN_TOKEN>`
        
        ## Data Flow
        1. Ingestion → 2. Scoring → 3. Chart Calculation → 4. Weekly Publication
        """,
        routes=app.routes,
    )
    
    # Add security schemes
    openapi_schema["components"]["securitySchemes"] = {
        "InternalToken": {
            "type": "apiKey",
            "in": "header",
            "name": "X-Internal-Token",
            "description": "For Cloudflare Workers and internal automation"
        },
        "BearerAuth": {
            "type": "http",
            "scheme": "bearer",
            "description": "Standard bearer token for ingestion clients"
        },
        "AdminToken": {
            "type": "http",
            "scheme": "bearer", 
            "description": "Admin access for publishing and management"
        }
    }
    
    # Tag endpoints
    for path, methods in openapi_schema["paths"].items():
        for method, details in methods.items():
            if "/admin/" in path:
                details["tags"] = ["Admin"]
            elif "/ingest/" in path:
                details["tags"] = ["Ingestion"]
            elif "/charts/" in path:
                details["tags"] = ["Charts"]
            else:
                details["tags"] = ["Health"]
    
    app.openapi_schema = openapi_schema
    return app.openapi_schema

app.openapi = custom_openapi
