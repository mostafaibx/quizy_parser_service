"""
FastAPI dependencies for dependency injection
Follows FastAPI best practices for shared resources
"""
from fastapi import Header, HTTPException, status
from typing import Optional
from app.config import get_settings

settings = get_settings()


async def verify_api_key(x_api_key: Optional[str] = Header(None)) -> str:
    """
    Dependency to verify API key if configured
    
    Usage:
        @router.get("/protected")
        async def protected_route(api_key: str = Depends(verify_api_key)):
            ...
    """
    # Skip verification if no API key is configured
    if not settings.API_KEY:
        return "no-auth-configured"
    
    if x_api_key != settings.API_KEY:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or missing API key"
        )
    
    return x_api_key


async def get_request_id(x_request_id: Optional[str] = Header(None)) -> Optional[str]:
    """
    Extract request ID from header if present
    
    Usage:
        @router.get("/endpoint")
        async def endpoint(request_id: str = Depends(get_request_id)):
            logger.info(f"Processing request {request_id}")
    """
    return x_request_id


def get_config():
    """
    Provide settings as a dependency
    
    Usage:
        @router.get("/info")
        async def info(config: Settings = Depends(get_config)):
            return {"environment": config.ENVIRONMENT}
    """
    return get_settings()

