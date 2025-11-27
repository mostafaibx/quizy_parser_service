"""
FastAPI application factory - Optimized for Cloud Functions
"""
from fastapi import FastAPI, Request, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.responses import JSONResponse
from contextlib import asynccontextmanager
import logging
import time
import uuid

from app import __version__
from app.config import get_settings

logger = logging.getLogger(__name__)
settings = get_settings()


@asynccontextmanager
async def lifespan(_app: FastAPI):
    """
    Manage application lifecycle
    Minimal startup/shutdown for Cloud Functions
    """
    # Startup
    logger.info("FastAPI app starting...")
    yield
    # Shutdown
    logger.info("FastAPI app shutting down...")


def create_app() -> FastAPI:
    """
    Create FastAPI application with minimal overhead
    Optimized for GCP Cloud Functions
    """
    app = FastAPI(
        title="Quizy Parser Service",
        description="High-performance document parsing service",
        version=__version__,
        docs_url="/docs" if settings.DEBUG else None,
        redoc_url="/redoc" if settings.DEBUG else None,
        openapi_url="/openapi.json" if settings.DEBUG else None,
        lifespan=lifespan
    )

    # Add global exception handler
    @app.exception_handler(Exception)
    async def global_exception_handler(_request: Request, exc: Exception):
        """Catch all unhandled exceptions"""
        logger.error("Unhandled exception: %s", exc, exc_info=True)
        return JSONResponse(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            content={
                "success": False,
                "error": {
                    "code": "INTERNAL_ERROR",
                    "message": "An unexpected error occurred" if settings.is_production() else str(exc)
                }
            }
        )

    # Add request logging middleware
    @app.middleware("http")
    async def log_requests(request: Request, call_next):
        """Log all requests with timing and request ID"""
        request_id = str(uuid.uuid4())
        request.state.request_id = request_id
        
        start_time = time.time()
        logger.info("Request started: %s %s [ID: %s]", request.method, request.url.path, request_id)
        
        response = await call_next(request)
        
        duration = time.time() - start_time
        logger.info(
            "Request completed: %s %s [ID: %s] [Status: %d] [Duration: %.2fms]",
            request.method, request.url.path, request_id, response.status_code, duration * 1000
        )
        
        # Add request ID to response headers
        response.headers["X-Request-ID"] = request_id
        
        return response

    # Add CORS middleware
    app.add_middleware(
        CORSMiddleware,
        allow_origins=settings.CORS_ORIGINS,
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    # Add GZip middleware for response compression
    app.add_middleware(GZipMiddleware, minimum_size=1000)

    # Register routes
    _register_routes(app)

    logger.info(
        "FastAPI app created [Environment: %s, Debug: %s, CORS Origins: %s]",
        settings.ENVIRONMENT, settings.DEBUG, settings.CORS_ORIGINS
    )

    return app


def _register_routes(app: FastAPI) -> None:
    """Register API routes"""
    from app.api.routes import health, parser, qstash_parser

    # Health check routes
    app.include_router(health.router, tags=["health"])

    # Parser routes (direct/sync)
    app.include_router(
        parser.router,
        prefix="/api/v1",
        tags=["parser"]
    )

    # QStash parser routes (async via queue)
    app.include_router(
        qstash_parser.router,
        prefix="/api/v1",
        tags=["qstash"]
    )