# src/main.py
from contextlib import asynccontextmanager
from typing import AsyncGenerator

from fastapi import FastAPI, Request, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.trustedhost import TrustedHostMiddleware
from fastapi.responses import JSONResponse
from fastapi.exceptions import RequestValidationError
import sentry_sdk
from prometheus_fastapi_instrumentator import Instrumentator

from src.config.settings import settings
from src.core.database import engine, init_db, close_db
from src.core.cache import init_cache, close_cache
from src.api.v1.routers import charts, artists, ingestion, admin


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator:
    """Lifespan manager for startup/shutdown events"""
    # Startup
    print(f"🚀 Starting {settings.project_name} v{settings.version}")
    
    # Initialize database
    await init_db()
    
    # Initialize cache
    await init_cache()
    
    # Initialize monitoring
    if settings.sentry_dsn:
        sentry_sdk.init(
            dsn=settings.sentry_dsn,
            traces_sample_rate=1.0 if settings.debug else 0.1,
            environment=settings.environment.value,
        )
    
    yield
    
    # Shutdown
    print("🛑 Shutting down...")
    await close_db()
    await close_cache()


def create_app() -> FastAPI:
    """Application factory pattern"""
    app = FastAPI(
        title=settings.project_name,
        version=settings.version,
        description="Production-ready Ugandan music chart engine",
        docs_url="/docs" if settings.debug else None,
        redoc_url="/redoc" if settings.debug else None,
        openapi_url="/openapi.json" if settings.debug else None,
        lifespan=lifespan,
    )
    
    # Add middleware
    app.add_middleware(
        CORSMiddleware,
        allow_origins=settings.security.cors_origins,
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )
    
    app.add_middleware(
        TrustedHostMiddleware,
        allowed_hosts=["*"] if settings.debug else settings.security.cors_origins,
    )
    
    # Add exception handlers
    @app.exception_handler(RequestValidationError)
    async def validation_exception_handler(request: Request, exc: RequestValidationError):
        return JSONResponse(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            content={
                "detail": exc.errors(),
                "body": exc.body,
            },
        )
    
    @app.exception_handler(Exception)
    async def general_exception_handler(request: Request, exc: Exception):
        # Log to Sentry
        if settings.sentry_dsn:
            sentry_sdk.capture_exception(exc)
        
        return JSONResponse(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            content={
                "detail": "Internal server error",
                "request_id": request.state.request_id if hasattr(request.state, "request_id") else None,
            },
        )
    
    # Include routers
    app.include_router(charts.router, prefix=f"{settings.api_prefix}/charts", tags=["charts"])
    app.include_router(artists.router, prefix=f"{settings.api_prefix}/artists", tags=["artists"])
    app.include_router(ingestion.router, prefix=f"{settings.api_prefix}/ingest", tags=["ingestion"])
    app.include_router(admin.router, prefix=f"{settings.api_prefix}/admin", tags=["admin"])
    
    # Health check endpoint
    @app.get("/health", tags=["health"])
    async def health_check():
        """Health check endpoint"""
        return {
            "status": "healthy",
            "service": settings.project_name,
            "version": settings.version,
            "timestamp": datetime.utcnow().isoformat(),
        }
    
    # Prometheus metrics
    if not settings.debug:
        Instrumentator().instrument(app).expose(app, endpoint="/metrics")
    
    return app


app = create_app()


if __name__ == "__main__":
    import uvicorn
    
    uvicorn.run(
        "src.main:app",
        host="0.0.0.0",
        port=8000,
        reload=settings.debug,
        log_level=settings.log_level.lower(),
    )
