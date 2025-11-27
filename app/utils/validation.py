"""
Validation utilities - Pure functions for input validation
"""
from pathlib import Path
from typing import List
import logging

logger = logging.getLogger(__name__)

# Try to import magic, but don't fail if not available
try:
    import magic
    HAS_MAGIC = True
except ImportError:
    logger.warning("python-magic not available, using file extension validation only")
    HAS_MAGIC = False

# Constants
MAX_FILE_SIZE = 10 * 1024 * 1024  # 10MB
ALLOWED_MIME_TYPES = [
    "application/pdf",
    "text/plain",
    "application/msword",
    "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
    "text/csv",
    "application/csv",
    "text/html",
    "application/xhtml+xml"
]


def validate_file_size(file_path: Path, max_size: int = MAX_FILE_SIZE) -> bool:
    """
    Validate file size

    Args:
        file_path: Path to file
        max_size: Maximum allowed size in bytes

    Returns:
        True if valid

    Raises:
        ValueError: If file is too large
    """
    file_size = file_path.stat().st_size
    if file_size > max_size:
        raise ValueError(f"File size {file_size} exceeds maximum {max_size} bytes")
    return True


def validate_mime_type(mime_type: str, allowed: List[str] = ALLOWED_MIME_TYPES) -> bool:
    """
    Validate MIME type

    Args:
        mime_type: MIME type to validate
        allowed: List of allowed MIME types

    Returns:
        True if valid

    Raises:
        ValueError: If MIME type is not allowed
    """
    if mime_type not in allowed:
        raise ValueError(f"MIME type {mime_type} is not supported")
    return True


def detect_file_type(file_path: Path) -> str:
    """
    Detect actual file type using python-magic or file extension

    Args:
        file_path: Path to file

    Returns:
        Detected MIME type
    """
    if HAS_MAGIC:
        try:
            mime = magic.from_file(str(file_path), mime=True)
            logger.debug(f"Detected MIME type: {mime} for {file_path}")
            return mime
        except Exception as e:
            logger.warning(f"Failed to detect file type: {e}")

    # Fallback to extension-based detection
    extension_map = {
        '.pdf': 'application/pdf',
        '.txt': 'text/plain',
        '.doc': 'application/msword',
        '.docx': 'application/vnd.openxmlformats-officedocument.wordprocessingml.document',
        '.csv': 'text/csv',
        '.html': 'text/html',
        '.htm': 'text/html'
    }

    suffix = file_path.suffix.lower()
    return extension_map.get(suffix, "application/octet-stream")


def validate_file_content(file_path: Path, expected_mime: str) -> bool:
    """
    Validate that file content matches expected MIME type

    Args:
        file_path: Path to file
        expected_mime: Expected MIME type

    Returns:
        True if content matches

    Raises:
        ValueError: If content doesn't match expected type
    """
    actual_mime = detect_file_type(file_path)

    # Allow some flexibility for similar types
    mime_aliases = {
        "application/pdf": ["application/pdf", "application/x-pdf"],
        "text/plain": ["text/plain", "text/x-plain"],
        "text/csv": ["text/csv", "application/csv", "text/plain"],
        "text/html": ["text/html", "application/xhtml+xml"]
    }

    allowed_types = mime_aliases.get(expected_mime, [expected_mime])

    if actual_mime not in allowed_types:
        raise ValueError(
            f"File content ({actual_mime}) doesn't match expected type ({expected_mime})"
        )

    return True


def sanitize_filename(filename: str) -> str:
    """
    Sanitize filename for safe storage

    Args:
        filename: Original filename

    Returns:
        Sanitized filename
    """
    import re

    # Remove or replace unsafe characters
    safe_name = re.sub(r'[^\w\s.-]', '', filename)
    safe_name = re.sub(r'\s+', '_', safe_name)

    # Ensure it's not empty
    if not safe_name:
        safe_name = "unnamed_file"

    return safe_name[:255]  # Limit length