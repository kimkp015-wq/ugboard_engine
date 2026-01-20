"""
Health check API endpoint for monitoring and deployment health checks.
"""
from datetime import datetime
from typing import Dict, Any
from fastapi import APIRouter, status
from fastapi.responses import JSONResponse

router = APIRouter(tags=["health"])

@router.get(
    "/health",
    summary="Health check endpoint",
    description="Returns service health status for monitoring",
    response_description="Service health information",
    status_code=status.HTTP_200_OK
)
async def health_check() -> JSONResponse:
    """
    Health check endpoint for load balancers and monitoring systems.
    
    Returns:
        JSONResponse: Service status with timestamp
    """
    health_status = {
        "status": "healthy",
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "service": "ugboard-engine",
        "version": "1.0.0"
    }
    
    return JSONResponse(
        content=health_status,
        status_code=status.HTTP_200_OK
    )


@router.get(
    "/health/detailed",
    summary="Detailed health check",
    description="Returns detailed service health including dependencies",
    response_description="Detailed health information",
    status_code=status.HTTP_200_OK
)
async def detailed_health_check() -> Dict[str, Any]:
    """
    Detailed health check including dependency status.
    
    Returns:
        dict: Detailed health status with dependency checks
    """
    # Add checks for your specific dependencies here
    # Example: database, external APIs, scraper status
    
    dependencies = {
        "api": "healthy",
        "scraper_system": "healthy",  # You can add actual checks
        "database": "not_configured",  # Update based on your setup
    }
    
    return {
        "status": "healthy",
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "service": "ugboard-engine",
        "version": "1.0.0",
        "dependencies": dependencies
    }
