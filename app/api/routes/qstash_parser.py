"""
QStash Parser Endpoint - Optimized for Upstash QStash integration
"""
from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel, Field
from typing import Dict, Any, Optional, List
import logging
import time
import traceback
from pathlib import Path

from app.parsers.registry import get_parser
from app.services.storage import download_file, upload_parsed_result, cleanup_temp_file
from app.utils.validation import validate_file_size, validate_mime_type

router = APIRouter()
logger = logging.getLogger(__name__)


class QStashParseRequest(BaseModel):
    """Request model for QStash parsing jobs"""
    file_id: str = Field(..., description="Unique file identifier")
    file_url: str = Field(..., description="Pre-signed R2 URL to download file")
    mime_type: str = Field(..., description="MIME type of the file")
    options: Dict[str, Any] = Field(default_factory=dict, description="Parsing options")
    user_id: Optional[str] = Field(None, description="User ID for organizing results")
    r2_bucket: Optional[str] = Field(None, description="R2 bucket name for saving results")


class QStashParseResponse(BaseModel):
    """Response model for QStash"""
    success: bool
    file_id: str
    message: str
    data: Optional[Dict[str, Any]] = None
    r2_key: Optional[str] = None
    error: Optional[Dict[str, Any]] = None
    processing_metrics: Optional[Dict[str, Any]] = None


@router.post("/parse/qstash", response_model=QStashParseResponse)
async def parse_from_qstash(
    request: QStashParseRequest,
    raw_request: Request
):
    """
    Parse document from QStash queue

    This endpoint is called by QStash when a job is dequeued.
    QStash will automatically retry on failure (status >= 500).

    Headers from QStash:
    - Upstash-Message-Id: Unique message ID
    - Upstash-Retried: Number of retries (0 on first attempt)
    - Upstash-Forward-To: Original destination URL
    """
    start_time = time.time()
    temp_file: Optional[Path] = None

    # Log QStash headers for debugging
    qstash_message_id = raw_request.headers.get("Upstash-Message-Id", "unknown")
    retry_count = raw_request.headers.get("Upstash-Retried", "0")

    logger.info(f"QStash job received: {qstash_message_id}, file: {request.file_id}, retry: {retry_count}")

    try:
        # Validate request
        validate_mime_type(request.mime_type)

        # Get parser
        parser = get_parser(request.mime_type)
        if not parser:
            # Return 4xx error - don't retry invalid requests
            return QStashParseResponse(
                success=False,
                file_id=request.file_id,
                message=f"No parser available for {request.mime_type}",
                error={"code": "INVALID_MIME_TYPE", "retryable": False}
            )

        # Download file from R2
        logger.info(f"Downloading file {request.file_id} from R2")
        temp_file = await download_file(request.file_url, request.file_id)

        # Validate file size
        try:
            validate_file_size(temp_file)
        except ValueError as e:
            # File too large - don't retry
            await cleanup_temp_file(temp_file)
            return QStashParseResponse(
                success=False,
                file_id=request.file_id,
                message=str(e),
                error={"code": "FILE_TOO_LARGE", "retryable": False}
            )

        # Parse document
        logger.info(f"Parsing {request.file_id} with {request.mime_type} parser")
        result = await parser(temp_file, request.options)

        # Add metadata
        result["documentId"] = request.file_id
        result["mimeType"] = request.mime_type

        # Upload parsed result to R2 if bucket specified
        r2_key = None
        if request.r2_bucket:
            # Organize by user if provided
            if request.user_id:
                r2_key = f"parsed/{request.user_id}/{request.file_id}/result.json"
            else:
                r2_key = f"parsed/{request.file_id}/result.json"

            try:
                await upload_parsed_result(result, request.file_id, request.r2_bucket)
                logger.info(f"Uploaded parsed result to R2: {r2_key}")
            except Exception as e:
                logger.warning(f"Failed to upload to R2: {e}")
                # Continue - parsing succeeded even if upload failed

        # Clean up temp file
        await cleanup_temp_file(temp_file)

        # Calculate metrics
        processing_time_ms = int((time.time() - start_time) * 1000)

        # Return success response
        # QStash will forward this to the callback URL
        return QStashParseResponse(
            success=True,
            file_id=request.file_id,
            message="Document parsed successfully",
            data=result,
            r2_key=r2_key,
            processing_metrics={
                "duration_ms": processing_time_ms,
                "pages_processed": len(result.get("pages", [])),
                "total_word_count": result.get("metadata", {}).get("totalWordCount", 0),
                "extraction_method": result.get("processingInfo", {}).get("extractionMethod", "unknown"),
                "retry_count": int(retry_count)
            }
        )

    except Exception as e:
        logger.error(f"Parse error for {request.file_id}: {str(e)}\n{traceback.format_exc()}")

        # Clean up on error
        if temp_file:
            await cleanup_temp_file(temp_file)

        # Determine if error is retryable
        retryable = True
        error_code = "PARSE_ERROR"

        # Don't retry certain errors
        if "corrupted" in str(e).lower():
            retryable = False
            error_code = "CORRUPTED_FILE"
        elif "unsupported" in str(e).lower():
            retryable = False
            error_code = "UNSUPPORTED_FORMAT"

        if not retryable:
            # Return 4xx - QStash won't retry
            return QStashParseResponse(
                success=False,
                file_id=request.file_id,
                message=f"Parse failed: {str(e)}",
                error={
                    "code": error_code,
                    "message": str(e),
                    "retryable": False
                }
            )
        else:
            # Return 5xx - QStash will retry
            raise HTTPException(
                status_code=500,
                detail={
                    "success": False,
                    "file_id": request.file_id,
                    "error": {
                        "code": error_code,
                        "message": str(e),
                        "retryable": True
                    }
                }
            ) from e


@router.get("/parse/health")
async def qstash_health_check():
    """
    Health check endpoint for QStash monitoring
    """
    return {
        "status": "healthy",
        "service": "qstash-parser",
        "timestamp": time.time()
    }


@router.post("/parse/batch/qstash")
async def parse_batch_from_qstash(
    requests: List[QStashParseRequest],
    raw_request: Request
):
    """
    Batch parsing endpoint for QStash

    Process multiple documents in a single job.
    Useful for reducing QStash message count.
    """
    qstash_message_id = raw_request.headers.get("Upstash-Message-Id", "unknown")
    logger.info(f"Batch QStash job received: {qstash_message_id}, files: {len(requests)}")

    results = []
    failures = []

    for parse_request in requests:
        temp_file: Optional[Path] = None
        try:
            # Validate request
            validate_mime_type(parse_request.mime_type)

            # Get parser
            parser = get_parser(parse_request.mime_type)
            if not parser:
                failures.append({
                    "file_id": parse_request.file_id,
                    "error": f"No parser available for {parse_request.mime_type}",
                    "code": "INVALID_MIME_TYPE"
                })
                continue

            # Download file
            temp_file = await download_file(parse_request.file_url, parse_request.file_id)

            # Validate file size
            try:
                validate_file_size(temp_file)
            except ValueError as e:
                await cleanup_temp_file(temp_file)
                failures.append({
                    "file_id": parse_request.file_id,
                    "error": str(e),
                    "code": "FILE_TOO_LARGE"
                })
                continue

            # Parse document
            result = await parser(temp_file, parse_request.options)

            # Add metadata
            result["documentId"] = parse_request.file_id
            result["mimeType"] = parse_request.mime_type

            # Upload to R2 if specified
            r2_key = None
            if parse_request.r2_bucket:
                if parse_request.user_id:
                    r2_key = f"parsed/{parse_request.user_id}/{parse_request.file_id}/result.json"
                else:
                    r2_key = f"parsed/{parse_request.file_id}/result.json"
                
                try:
                    await upload_parsed_result(result, parse_request.file_id, parse_request.r2_bucket)
                except Exception as upload_error:
                    logger.warning(f"Failed to upload batch result to R2: {upload_error}")

            await cleanup_temp_file(temp_file)

            results.append({
                "file_id": parse_request.file_id,
                "success": True,
                "r2_key": r2_key,
                "pages_processed": len(result.get("pages", []))
            })

        except Exception as e:
            logger.error(f"Batch parse error for {parse_request.file_id}: {str(e)}\n{traceback.format_exc()}")
            
            # Clean up on error
            if temp_file:
                await cleanup_temp_file(temp_file)
            
            failures.append({
                "file_id": parse_request.file_id,
                "error": str(e),
                "code": "PARSE_ERROR"
            })

    # Return batch results
    return {
        "success": len(failures) == 0,
        "processed": len(results),
        "failed": len(failures),
        "results": results,
        "failures": failures
    }