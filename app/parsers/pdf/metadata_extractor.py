"""
PDF Metadata Extractor - Extracts comprehensive metadata from PDFs
Includes document properties, structure info, and page formats
"""
from pathlib import Path
from typing import Dict, Any, Optional, List
import logging
from datetime import datetime
from functools import lru_cache

logger = logging.getLogger(__name__)


@lru_cache(maxsize=1)
def _get_fitz():
    import fitz
    return fitz



def extract_pdf_metadata(file_path: Path) -> Dict[str, Any]:
    """
    Extract comprehensive metadata from PDF
    
    Extracts:
    - Standard metadata (title, author, subject, keywords, creator, producer)
    - Dates (creation, modification)
    - Document structure (pages, encryption, forms, annotations, outline)
    - Page information (size, format)
    
    Args:
        file_path: Path to PDF file
    
    Returns:
        Dictionary with metadata fields (only non-null values)
    """
    fitz = _get_fitz()
    metadata = {}

    try:
        doc = fitz.open(file_path)

        # Extract standard metadata
        fitz_meta = doc.metadata  # type: ignore[attr-defined]
        if fitz_meta:
            metadata["title"] = _clean_string(fitz_meta.get('title'))
            metadata["author"] = _clean_string(fitz_meta.get('author'))
            metadata["subject"] = _clean_string(fitz_meta.get('subject'))
            metadata["keywords"] = _extract_keywords(fitz_meta.get('keywords', ''))
            metadata["creator"] = _clean_string(fitz_meta.get('creator'))
            metadata["producer"] = _clean_string(fitz_meta.get('producer'))
            metadata["creation_date"] = _parse_date(fitz_meta.get('creationDate'))
            metadata["modification_date"] = _parse_date(fitz_meta.get('modDate'))

        # Document structure info
        metadata["num_pages"] = doc.page_count
        metadata["has_forms"] = any(page.first_widget for page in doc)
        metadata["has_annotations"] = any(page.first_annot for page in doc)

        # Page information
        if doc.page_count > 0:
            first_page = doc[0]
            metadata["page_size"] = {
                "width": first_page.rect.width,
                "height": first_page.rect.height,
                "format": _identify_page_format(first_page.rect.width, first_page.rect.height)
            }

        # Check for outline (bookmarks/table of contents)
        try:
            toc = doc.get_toc()
            metadata["has_outline"] = len(toc) > 0
            metadata["outline_depth"] = max([level for level, title, page in toc]) if toc else 0
        except Exception as e:
            logger.debug("Failed to extract outline: %s", e)
            metadata["has_outline"] = False
            metadata["outline_depth"] = 0

        doc.close()

        # Clean up metadata (remove None values)
        return {k: v for k, v in metadata.items() if v is not None}

    except Exception as e:
        logger.warning("Metadata extraction failed: %s", e)
        return {}


def _clean_string(value: Any) -> Optional[str]:
    """
    Clean and normalize string values from PDF metadata
    
    Removes:
    - Leading/trailing whitespace
    - Null characters (\x00)
    - Empty strings
    
    Args:
        value: Raw metadata value
    
    Returns:
        Cleaned string or None if empty/invalid
    """
    if value is None:
        return None

    str_value = str(value).strip()
    str_value = str_value.replace('\x00', '')  # Remove null characters

    return str_value if str_value else None


def _parse_date(date_str: Any) -> Optional[str]:
    """
    Parse PDF date format to ISO 8601
    
    PDF dates follow format: D:YYYYMMDDHHmmSSOHH'mm
    Example: D:20231125143000+05'30
    
    Args:
        date_str: PDF date string
    
    Returns:
        ISO 8601 formatted date string (YYYY-MM-DDTHH:mm:ss) or None
    """
    if not date_str:
        return None

    try:
        date_str = str(date_str)

        # Remove D: prefix (PDF standard format)
        if date_str.startswith('D:'):
            date_str = date_str[2:]

        # Parse date/time components
        year = date_str[0:4]
        month = date_str[4:6] if len(date_str) > 4 else '01'
        day = date_str[6:8] if len(date_str) > 6 else '01'
        hour = date_str[8:10] if len(date_str) > 8 else '00'
        minute = date_str[10:12] if len(date_str) > 10 else '00'
        second = date_str[12:14] if len(date_str) > 12 else '00'

        dt = datetime(
            int(year), int(month), int(day),
            int(hour), int(minute), int(second)
        )

        return dt.isoformat()

    except Exception as e:
        logger.debug("Failed to parse date '%s': %s", date_str, e)
        return str(date_str) if date_str else None


def _extract_keywords(keywords_str: Any) -> List[str]:
    """
    Extract keywords from PDF metadata string
    
    Handles multiple separator formats:
    - Comma-separated: "keyword1, keyword2, keyword3"
    - Semicolon-separated: "keyword1; keyword2; keyword3"
    - Pipe-separated: "keyword1 | keyword2 | keyword3"
    - Space-separated: "keyword1 keyword2 keyword3"
    
    Args:
        keywords_str: Raw keywords string from PDF metadata
    
    Returns:
        List of cleaned keyword strings (min 2 chars each)
    """
    if not keywords_str:
        return []

    keywords_str = str(keywords_str)

    # Try different separators (in order of preference)
    for separator in [',', ';', '|']:
        if separator in keywords_str:
            keywords = keywords_str.split(separator)
            return [k.strip() for k in keywords if k.strip() and len(k.strip()) > 1]

    # Fallback to space-separated or single keyword
    keywords = keywords_str.split()
    return [k.strip() for k in keywords if k.strip() and len(k.strip()) > 1]


def _identify_page_format(width: float, height: float) -> str:
    """
    Identify common page formats (US Letter, A4, Legal, etc.)
    
    Standard formats (in PostScript points, 1pt = 1/72 inch):
    - Letter: 8.5" x 11" (612 x 792 pts)
    - Legal: 8.5" x 14" (612 x 1008 pts)
    - A4: 210mm x 297mm (595 x 842 pts)
    - A3: 297mm x 420mm (842 x 1191 pts)
    - A5: 148mm x 210mm (420 x 595 pts)
    
    Args:
        width: Page width in points
        height: Page height in points
    
    Returns:
        Page format name (e.g., "A4", "Letter") or "Custom (Portrait/Landscape)"
    """
    formats = {
        "Letter": (612, 792),
        "Legal": (612, 1008),
        "A4": (595, 842),
        "A3": (842, 1191),
        "A5": (420, 595),
    }

    # Check for matches (within 10 points tolerance for both orientations)
    for format_name, (std_width, std_height) in formats.items():
        # Portrait orientation
        if abs(width - std_width) < 10 and abs(height - std_height) < 10:
            return format_name
        # Landscape orientation
        if abs(width - std_height) < 10 and abs(height - std_width) < 10:
            return format_name

    # Custom format - determine orientation
    return "Custom (Landscape)" if width > height else "Custom (Portrait)"