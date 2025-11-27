"""
Temporary File Manager for Cloud Functions
Handles cleanup and manages limited /tmp space
"""
import os
import tempfile
import shutil
from pathlib import Path
from contextlib import contextmanager
from typing import Optional, Generator
import logging
import atexit

logger = logging.getLogger(__name__)

# Track temp directories for cleanup
_temp_dirs = set()

def cleanup_all_temps():
    """Clean up all temporary directories"""
    for temp_dir in list(_temp_dirs):
        try:
            if os.path.exists(temp_dir):
                shutil.rmtree(temp_dir)
                logger.debug(f"Cleaned up temp dir: {temp_dir}")
        except Exception as e:
            logger.warning(f"Failed to cleanup {temp_dir}: {e}")
        finally:
            _temp_dirs.discard(temp_dir)

# Register cleanup on exit
atexit.register(cleanup_all_temps)


@contextmanager
def temp_directory(prefix: str = "pdf_", cleanup: bool = True) -> Generator[Path, None, None]:
    """
    Context manager for temporary directory with automatic cleanup

    Args:
        prefix: Directory prefix
        cleanup: Whether to cleanup on exit

    Yields:
        Path to temporary directory
    """
    temp_dir = None
    try:
        # Create temp directory in /tmp
        temp_dir = tempfile.mkdtemp(prefix=prefix, dir="/tmp")
        temp_path = Path(temp_dir)

        # Track for cleanup
        _temp_dirs.add(temp_dir)

        logger.debug(f"Created temp directory: {temp_dir}")
        yield temp_path

    finally:
        if cleanup and temp_dir and os.path.exists(temp_dir):
            try:
                shutil.rmtree(temp_dir)
                _temp_dirs.discard(temp_dir)
                logger.debug(f"Cleaned up temp directory: {temp_dir}")
            except Exception as e:
                logger.warning(f"Failed to cleanup temp directory: {e}")


@contextmanager
def temp_file(suffix: str = ".pdf", prefix: str = "tmp_", cleanup: bool = True) -> Generator[Path, None, None]:
    """
    Context manager for temporary file with automatic cleanup

    Args:
        suffix: File suffix
        prefix: File prefix
        cleanup: Whether to cleanup on exit

    Yields:
        Path to temporary file
    """
    fd, temp_path = tempfile.mkstemp(suffix=suffix, prefix=prefix, dir="/tmp")
    temp_file_path = Path(temp_path)

    try:
        os.close(fd)  # Close file descriptor
        yield temp_file_path
    finally:
        if cleanup and temp_file_path.exists():
            try:
                temp_file_path.unlink()
                logger.debug(f"Cleaned up temp file: {temp_file_path}")
            except Exception as e:
                logger.warning(f"Failed to cleanup temp file: {e}")


def get_tmp_usage() -> dict:
    """Get /tmp directory usage statistics"""
    stat = shutil.disk_usage("/tmp")
    return {
        "total_mb": stat.total // (1024 * 1024),
        "used_mb": stat.used // (1024 * 1024),
        "free_mb": stat.free // (1024 * 1024),
        "percent_used": (stat.used / stat.total) * 100
    }


def ensure_tmp_space(required_mb: int = 50) -> bool:
    """
    Ensure enough space in /tmp

    Args:
        required_mb: Required space in MB

    Returns:
        True if enough space available
    """
    usage = get_tmp_usage()

    if usage["free_mb"] < required_mb:
        logger.warning(f"Low /tmp space: {usage['free_mb']}MB free, {required_mb}MB required")

        # Try to cleanup old temps
        cleanup_all_temps()

        # Check again
        usage = get_tmp_usage()
        if usage["free_mb"] < required_mb:
            logger.error(f"Insufficient /tmp space after cleanup: {usage['free_mb']}MB free")
            return False

    return True


def cleanup_old_files(max_age_seconds: int = 3600):
    """Clean up old files in /tmp older than max_age_seconds"""
    import time

    tmp_dir = Path("/tmp")
    current_time = time.time()

    for item in tmp_dir.iterdir():
        try:
            # Skip if not our temp file/dir
            if not (item.name.startswith("pdf_") or item.name.startswith("tmp_")):
                continue

            # Check age
            age = current_time - item.stat().st_mtime
            if age > max_age_seconds:
                if item.is_dir():
                    shutil.rmtree(item)
                else:
                    item.unlink()
                logger.debug(f"Cleaned up old temp: {item}")

        except Exception as e:
            logger.debug(f"Failed to cleanup {item}: {e}")