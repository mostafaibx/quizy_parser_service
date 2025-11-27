"""
Health check endpoints - Lightweight for quick responses
"""
from fastapi import APIRouter, status
from app import __version__
from app.models.schema import HealthResponse
from app.parsers.registry import get_supported_formats
import time

router = APIRouter()

# Module-level start time for uptime calculation
_start_time = time.time()


@router.get("/health", response_model=HealthResponse, status_code=status.HTTP_200_OK)
async def health_check():
    """Basic health check endpoint - minimal processing"""
    return HealthResponse(
        status="healthy",
        version=__version__,
        parsers={
            format_name: "ready"
            for format_name in get_supported_formats().keys()
        }
    )


@router.get("/health/ready", status_code=status.HTTP_200_OK)
async def readiness_check():
    """Readiness probe for Cloud Run/Functions"""
    return {
        "ready": True,
        "uptime_seconds": int(time.time() - _start_time)
    }


@router.get("/health/live", status_code=status.HTTP_200_OK)
async def liveness_check():
    """Liveness probe - always returns success if service is running"""
    return {"alive": True}