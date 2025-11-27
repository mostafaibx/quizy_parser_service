"""
Storage service - Functional utilities for file operations with Cloudflare R2
Optimized for Cloud Functions with minimal dependencies
"""
from pathlib import Path
from typing import Optional, Dict, Any
import aiofiles
import httpx
import tempfile
import logging
import os
import hashlib
import boto3
from botocore.client import Config

logger = logging.getLogger(__name__)

# Configuration
TEMP_DIR = Path(tempfile.gettempdir()) / "parser_temp"
MAX_DOWNLOAD_SIZE = 10 * 1024 * 1024  # 10MB
DOWNLOAD_TIMEOUT = 30  # seconds

# Ensure temp directory exists
TEMP_DIR.mkdir(exist_ok=True, parents=True)

# R2 client cache (module level for reuse across invocations)
_r2_client = None


def get_r2_client():
    """
    Get or create R2 client using boto3 (S3-compatible API)
    Lazy loaded to reduce cold start time
    """
    global _r2_client
    if _r2_client is None:
        # R2 credentials from environment
        account_id = os.getenv("R2_ACCOUNT_ID")
        access_key = os.getenv("R2_ACCESS_KEY_ID")
        secret_key = os.getenv("R2_SECRET_ACCESS_KEY")

        if not all([account_id, access_key, secret_key]):
            logger.warning("R2 credentials not configured, storage operations will fail")
            return None

        _r2_client = boto3.client(
            service_name="s3",
            endpoint_url=f"https://{account_id}.r2.cloudflarestorage.com",
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            config=Config(
                signature_version="s3v4",
                retries={"max_attempts": 3, "mode": "adaptive"}
            )
        )
    return _r2_client


async def download_file(url: str, file_id: str) -> Path:
    """
    Download file from URL (pre-signed R2 URL or direct URL)
    For local testing, also supports local file paths

    Args:
        url: Pre-signed URL to download from or local file path
        file_id: Unique identifier for the file

    Returns:
        Path to downloaded temporary file

    Raises:
        ValueError: If download fails or file is too large
    """
    # Convert Pydantic HttpUrl to string
    url_str = str(url)
    
    # Check if URL is actually a local file path (for testing)
    if url_str.startswith('/') or url_str.startswith('./') or Path(url_str).exists():
        local_path = Path(url_str)
        if local_path.exists():
            logger.info("Using local file for testing: %s", local_path)
            # Copy to temp directory
            temp_path = TEMP_DIR / f"{file_id}_{local_path.name}"
            import shutil
            shutil.copy2(local_path, temp_path)
            return temp_path
        else:
            raise ValueError(f"Local file not found: {url_str}")

    # Normal URL download
    temp_path = TEMP_DIR / f"{file_id}_{hashlib.md5(url_str.encode()).hexdigest()[:8]}"

    try:
        async with httpx.AsyncClient(timeout=DOWNLOAD_TIMEOUT) as client:
            response = await client.get(url_str)
            response.raise_for_status()

            # Check content length
            content_length = int(response.headers.get("content-length", 0))
            if content_length > MAX_DOWNLOAD_SIZE:
                raise ValueError(f"File too large: {content_length} bytes")

            # Write to temporary file
            async with aiofiles.open(temp_path, "wb") as f:
                await f.write(response.content)

            logger.info("Downloaded %d bytes to %s", len(response.content), temp_path)
            return temp_path

    except httpx.RequestError as e:
        logger.error("Failed to download file: %s", e)
        raise ValueError(f"Download failed: {str(e)}")
    except Exception as e:
        # Clean up on error
        if temp_path.exists():
            temp_path.unlink(missing_ok=True)
        raise


async def download_from_r2(bucket_name: str, key: str, file_id: str) -> Path:
    """
    Download file directly from R2 bucket

    Args:
        bucket_name: R2 bucket name
        key: Object key in bucket
        file_id: Unique identifier for caching

    Returns:
        Path to downloaded file
    """
    temp_path = TEMP_DIR / f"{file_id}_{hashlib.md5(key.encode()).hexdigest()[:8]}"

    try:
        client = get_r2_client()
        if not client:
            raise ValueError("R2 client not configured")

        # Get object metadata first to check size
        response = client.head_object(Bucket=bucket_name, Key=key)
        content_length = response.get("ContentLength", 0)

        if content_length > MAX_DOWNLOAD_SIZE:
            raise ValueError(f"File too large: {content_length} bytes")

        # Download file
        client.download_file(bucket_name, key, str(temp_path))
        logger.info(f"Downloaded {content_length} bytes from R2 to {temp_path}")

        return temp_path

    except Exception as e:
        if temp_path.exists():
            temp_path.unlink(missing_ok=True)
        logger.error(f"Failed to download from R2: {e}")
        raise


async def upload_to_r2(
    file_path: Path,
    bucket_name: str,
    key: str,
    content_type: str = "application/json",
    metadata: Optional[Dict[str, str]] = None
) -> str:
    """
    Upload file to R2 bucket

    Args:
        file_path: Local file to upload
        bucket_name: R2 bucket name
        key: Object key in bucket
        content_type: MIME type of the file
        metadata: Optional metadata to attach

    Returns:
        R2 key of uploaded file
    """
    try:
        client = get_r2_client()
        if not client:
            raise ValueError("R2 client not configured")

        extra_args = {"ContentType": content_type}
        if metadata:
            extra_args["Metadata"] = metadata

        client.upload_file(
            str(file_path),
            bucket_name,
            key,
            ExtraArgs=extra_args
        )

        logger.info(f"Uploaded {file_path} to R2: {bucket_name}/{key}")
        return key

    except Exception as e:
        logger.error(f"Failed to upload to R2: {e}")
        raise


async def upload_parsed_result(
    result: Dict[str, Any],
    file_id: str,
    bucket_name: str
) -> str:
    """
    Upload parsed result to R2 as JSON

    Args:
        result: Parsed document data
        file_id: Original file ID
        bucket_name: R2 bucket name

    Returns:
        R2 key of uploaded result
    """
    import json

    # Create temporary JSON file
    temp_path = TEMP_DIR / f"{file_id}_parsed.json"

    try:
        # Write result to temp file
        async with aiofiles.open(temp_path, "w") as f:
            await f.write(json.dumps(result, ensure_ascii=False, indent=2))

        # Upload to R2
        key = f"parsed/{file_id}/result.json"
        await upload_to_r2(
            temp_path,
            bucket_name,
            key,
            content_type="application/json",
            metadata={
                "file_id": file_id,
                "type": "parsed_document"
            }
        )

        return key

    finally:
        # Clean up temp file
        if temp_path.exists():
            temp_path.unlink(missing_ok=True)


def generate_presigned_url(
    bucket_name: str,
    key: str,
    expiration: int = 3600,
    http_method: str = "GET"
) -> str:
    """
    Generate pre-signed URL for R2 object

    Args:
        bucket_name: R2 bucket name
        key: Object key
        expiration: URL expiration in seconds
        http_method: HTTP method (GET or PUT)

    Returns:
        Pre-signed URL
    """
    client = get_r2_client()
    if not client:
        raise ValueError("R2 client not configured")

    url = client.generate_presigned_url(
        ClientMethod="get_object" if http_method == "GET" else "put_object",
        Params={"Bucket": bucket_name, "Key": key},
        ExpiresIn=expiration
    )

    return url


async def cleanup_temp_file(file_path: Optional[Path]) -> None:
    """
    Clean up temporary file

    Args:
        file_path: Path to temporary file to delete
    """
    if file_path and file_path.exists():
        try:
            file_path.unlink()
            logger.debug(f"Cleaned up temp file: {file_path}")
        except Exception as e:
            logger.warning(f"Failed to cleanup temp file {file_path}: {e}")


def cleanup_old_temp_files(max_age_seconds: int = 3600) -> int:
    """
    Clean up old temporary files

    Args:
        max_age_seconds: Maximum age of files to keep

    Returns:
        Number of files cleaned up
    """
    import time

    if not TEMP_DIR.exists():
        return 0

    current_time = time.time()
    cleaned = 0

    for file_path in TEMP_DIR.iterdir():
        if file_path.is_file():
            file_age = current_time - file_path.stat().st_mtime
            if file_age > max_age_seconds:
                try:
                    file_path.unlink()
                    cleaned += 1
                except Exception as e:
                    logger.warning(f"Failed to cleanup old file {file_path}: {e}")

    if cleaned > 0:
        logger.info(f"Cleaned up {cleaned} old temporary files")

    return cleaned