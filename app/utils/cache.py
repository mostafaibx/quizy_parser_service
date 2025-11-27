"""
Lightweight caching utilities for Cloud Functions
Using module-level caching that persists across invocations
"""
from typing import Any, Optional, Dict, Tuple
import time
import hashlib
import json
import logging

logger = logging.getLogger(__name__)

# Module-level cache - survives across function invocations
_cache: Dict[str, Tuple[Any, float]] = {}
_cache_hits = 0
_cache_misses = 0

# Configuration
MAX_CACHE_SIZE = 100  # Maximum number of cached items
DEFAULT_TTL = 900  # 15 minutes default TTL


def get_cache_key(file_id: str, options: Dict[str, Any]) -> str:
    """Generate cache key from file ID and options"""
    options_str = json.dumps(options, sort_keys=True)
    combined = f"{file_id}:{options_str}"
    return hashlib.md5(combined.encode()).hexdigest()


def get_from_cache(key: str) -> Optional[Any]:
    """
    Get value from cache if exists and not expired

    Args:
        key: Cache key

    Returns:
        Cached value or None if not found/expired
    """
    global _cache_hits, _cache_misses

    if key in _cache:
        value, expiry = _cache[key]
        if time.time() < expiry:
            _cache_hits += 1
            logger.debug(f"Cache hit for key: {key}")
            return value
        else:
            # Expired, remove from cache
            del _cache[key]

    _cache_misses += 1
    logger.debug(f"Cache miss for key: {key}")
    return None


def set_in_cache(key: str, value: Any, ttl: int = DEFAULT_TTL) -> None:
    """
    Set value in cache with TTL

    Args:
        key: Cache key
        value: Value to cache
        ttl: Time to live in seconds
    """
    global _cache

    # Implement simple LRU by removing oldest entries if cache is full
    if len(_cache) >= MAX_CACHE_SIZE:
        _evict_oldest()

    expiry = time.time() + ttl
    _cache[key] = (value, expiry)
    logger.debug(f"Cached value for key: {key}, TTL: {ttl}s")


def clear_cache() -> None:
    """Clear all cached items"""
    global _cache, _cache_hits, _cache_misses
    _cache.clear()
    _cache_hits = 0
    _cache_misses = 0
    logger.info("Cache cleared")


def get_cache_stats() -> Dict[str, Any]:
    """Get cache statistics"""
    return {
        "size": len(_cache),
        "max_size": MAX_CACHE_SIZE,
        "hits": _cache_hits,
        "misses": _cache_misses,
        "hit_rate": _cache_hits / (_cache_hits + _cache_misses) if (_cache_hits + _cache_misses) > 0 else 0
    }


def _evict_oldest() -> None:
    """Evict oldest cached items (LRU)"""
    if not _cache:
        return

    # Find and remove the item with earliest expiry
    oldest_key = min(_cache.keys(), key=lambda k: _cache[k][1])
    del _cache[oldest_key]
    logger.debug(f"Evicted oldest cache entry: {oldest_key}")


# OCR result caching
_ocr_cache: Dict[str, str] = {}
MAX_OCR_CACHE = 50


def get_ocr_cache_key(image_hash: str, language: str) -> str:
    """Generate cache key for OCR results"""
    return f"ocr:{image_hash}:{language}"


def cache_ocr_result(image_hash: str, language: str, text: str) -> None:
    """Cache OCR result"""
    global _ocr_cache

    if len(_ocr_cache) >= MAX_OCR_CACHE:
        # Remove oldest entry (simple FIFO)
        _ocr_cache.pop(next(iter(_ocr_cache)))

    key = get_ocr_cache_key(image_hash, language)
    _ocr_cache[key] = text
    logger.debug(f"Cached OCR result for {image_hash}")


def get_cached_ocr(image_hash: str, language: str) -> Optional[str]:
    """Get cached OCR result"""
    key = get_ocr_cache_key(image_hash, language)
    return _ocr_cache.get(key)