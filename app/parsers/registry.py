"""
Parser Registry - Lazy loading parser modules for optimal cold start
Caches loaded parsers globally to reuse across GCP Cloud Function invocations
"""
from typing import Dict, Callable, Optional, Any, List
import logging

logger = logging.getLogger(__name__)

# Global parser cache - survives across Cloud Function invocations
_parsers: Dict[str, Callable] = {}
_parser_info: Dict[str, Dict[str, Any]] = {}


def get_parser(mime_type: str) -> Optional[Callable]:
    """
    Get parser function for given MIME type with lazy loading
    
    Lazy loads parsers only when first requested and caches them globally.
    This optimizes GCP Cloud Function cold start times.
    
    Args:
        mime_type: MIME type of document (e.g., 'application/pdf')
    
    Returns:
        Parser function (async callable) or None if unsupported
        
    Supported parsers:
        - PDF: Comprehensive multi-strategy parser with metadata-driven approach
        - Word: DOCX/DOC parser (future implementation)
        - Text: Plain text parser (future implementation)
        - CSV: CSV/spreadsheet parser (future implementation)
        - HTML: HTML/web content parser (future implementation)
    """
    # Check cache first (fast path for warm starts)
    if mime_type in _parsers:
        return _parsers[mime_type]

    # Lazy load based on MIME type (only on first request)
    parser = None
    info = {}

    if mime_type == "application/pdf":
        from app.parsers.pdf_parser import parse_pdf
        parser = parse_pdf
        info = {"name": "PDF Parser", "version": "1.0", "formats": ["pdf"]}

    elif mime_type in ["application/vnd.openxmlformats-officedocument.wordprocessingml.document",
                        "application/msword"]:
        try:
            from app.parsers.word_parser import parse_word  # type: ignore[import]
            parser = parse_word
            info = {"name": "Word Parser", "version": "1.0", "formats": ["docx", "doc"]}
        except ImportError:
            logger.debug("Word parser not yet implemented")

    elif mime_type == "text/plain":
        try:
            from app.parsers.text_parser import parse_text  # type: ignore[import]
            parser = parse_text
            info = {"name": "Text Parser", "version": "1.0", "formats": ["txt"]}
        except ImportError:
            logger.debug("Text parser not yet implemented")

    elif mime_type in ["text/csv", "application/csv"]:
        try:
            from app.parsers.csv_parser import parse_csv  # type: ignore[import]
            parser = parse_csv
            info = {"name": "CSV Parser", "version": "1.0", "formats": ["csv"]}
        except ImportError:
            logger.debug("CSV parser not yet implemented")

    elif mime_type in ["text/html", "application/xhtml+xml"]:
        try:
            from app.parsers.html_parser import parse_html  # type: ignore[import]
            parser = parse_html
            info = {"name": "HTML Parser", "version": "1.0", "formats": ["html", "htm"]}
        except ImportError:
            logger.debug("HTML parser not yet implemented")

    # Cache successful parser load
    if parser:
        _parsers[mime_type] = parser
        _parser_info[mime_type] = info
        logger.info("Loaded parser for %s: %s", mime_type, info['name'])

    return parser


def get_supported_formats() -> Dict[str, List[str]]:
    """
    Get all supported formats without loading parsers
    
    Returns lightweight format mapping without triggering lazy imports.
    Useful for API documentation and format validation.
    
    Returns:
        Dictionary mapping format names to MIME type lists
        
    Note:
        Only PDF parser is currently implemented.
        Other parsers are listed for future implementation.
    """
    return {
        "pdf": ["application/pdf"],  # ✅ Implemented
        "word": [  # 🔜 Future implementation
            "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
            "application/msword"
        ],
        "text": ["text/plain"],  # 🔜 Future implementation
        "csv": ["text/csv", "application/csv"],  # 🔜 Future implementation
        "html": ["text/html", "application/xhtml+xml"]  # 🔜 Future implementation
    }


def clear_parser_cache() -> None:
    """
    Clear parser cache (useful for testing and development)
    
    Clears the global parser cache to force re-loading on next request.
    Primarily used in test scenarios to ensure clean state.
    
    Note:
        In production GCP Cloud Functions, the cache persists across
        warm invocations which is the desired behavior for performance.
    """
    _parsers.clear()
    _parser_info.clear()
    logger.debug("Parser cache cleared")