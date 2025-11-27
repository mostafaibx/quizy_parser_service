"""
Parser API endpoints - Functional approach optimized for Cloud Functions
"""
from fastapi import APIRouter, HTTPException, BackgroundTasks
from app.models.schema import ParseRequest, ParseResponse
from app.parsers.registry import get_parser, get_supported_formats
from app.services.storage import download_file, cleanup_temp_file
from app.utils.cache import get_from_cache, set_in_cache, get_cache_key, get_cache_stats
from app.utils.validation import validate_file_size, validate_mime_type
import logging
import time
import traceback
from pathlib import Path
from typing import Optional

router = APIRouter()
logger = logging.getLogger(__name__)


@router.post("/parse", response_model=ParseResponse)
async def parse_document(
    request: ParseRequest,
    background_tasks: BackgroundTasks
):
    """
    Parse document from URL
    Optimized for minimal processing overhead
    """
    start_time = time.time()
    temp_file: Optional[Path] = None

    try:
        # Check cache first
        cache_key = get_cache_key(request.file_id, request.options)
        cached_result = get_from_cache(cache_key)

        if cached_result:
            logger.info(f"Returning cached result for {request.file_id}")
            return ParseResponse(
                success=True,
                data=cached_result,
                processing_metrics={
                    "duration_ms": int((time.time() - start_time) * 1000),
                    "cache_hit": True
                }
            )

        # Validate request
        validate_mime_type(request.mime_type)

        # Get appropriate parser
        parser = get_parser(request.mime_type)
        if not parser:
            raise HTTPException(
                status_code=400,
                detail=f"No parser available for MIME type: {request.mime_type}"
            )

        # Download file
        logger.info(f"Downloading file {request.file_id} from {request.file_url}")
        temp_file = await download_file(request.file_url, request.file_id)

        # Validate file size
        validate_file_size(temp_file)

        # Parse document
        logger.info(f"Parsing {request.file_id} with {request.mime_type} parser")
        result = await parser(temp_file, request.options)

        # Add document metadata
        result["documentId"] = request.file_id
        result["mimeType"] = request.mime_type

        # Cache successful result
        set_in_cache(cache_key, result, ttl=900)  # 15 minutes

        # Schedule cleanup in background
        background_tasks.add_task(cleanup_temp_file, temp_file)

        # Return response
        return ParseResponse(
            success=True,
            data=result,
            processing_metrics={
                "duration_ms": int((time.time() - start_time) * 1000),
                "pages_processed": len(result.get("pages", [])),
                "cache_hit": False
            }
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Parse error for {request.file_id}: {str(e)}\n{traceback.format_exc()}")

        # Clean up on error
        if temp_file:
            background_tasks.add_task(cleanup_temp_file, temp_file)

        return ParseResponse(
            success=False,
            error={
                "code": "PARSE_ERROR",
                "message": str(e),
                "retry_able": True
            }
        )


@router.get("/parse/formats")
async def get_supported_formats_endpoint():
    """Get list of supported parsing formats"""
    formats = get_supported_formats()
    return {
        "formats": formats,
        "max_file_size_mb": 10,
        "cache_enabled": True
    }


@router.get("/cache/stats")
async def get_cache_statistics():
    """Get cache statistics (useful for monitoring)"""
    return get_cache_stats()