"""
PDF Parser - Entry point for comprehensive PDF parsing
Delegates to the multi-strategy PDF parser module
"""
from pathlib import Path
from typing import Dict, Any, Optional

# Import the comprehensive PDF parser
from app.parsers.pdf import parse_pdf as _parse_pdf


async def parse_pdf(
    file_path: Path,
    options: Optional[Dict[str, Any]] = None
) -> Dict[str, Any]:
    """
    Parse PDF document using comprehensive multi-strategy approach
    
    This is a metadata-driven parser that uses client-provided information
    to optimize extraction and processing.
    
    REQUIRED metadata fields (must be provided in options):
    - language: str - Document language ('en', 'ar', 'de', 'fr', 'es', 'it')
    - subject: str - Academic subject (see app.constants.SUPPORTED_SUBJECTS)
    - document_type: str - Type ('explanation', 'exercises', 'mixed')
    
    Optional metadata fields:
    - academic_level: str - Educational level ('primary', 'secondary', 'high', 'college')
    
    Processing options:
    - extract_images: bool - Extract images from PDF (default: False)
    - extract_tables: bool - Extract tables from PDF (default: True)
    - ocr_enabled: bool - Enable OCR for scanned PDFs (default: False)
    - max_pages: int - Maximum pages to process (default: all)
    - force_strategy: str - Override auto-detected strategy
    - generate_quiz_content: bool - Generate quiz-ready content (default: False)
    
    Extraction strategies (auto-selected based on PDF characteristics):
    - text_focus: Fast text extraction for text-heavy PDFs
    - hybrid: Mixed native text + OCR for partially scanned PDFs
    - table_focus: Optimized for data-heavy PDFs with tables
    - math_focus: Specialized for math/science PDFs with equations
    - ocr_heavy: Full OCR processing for scanned documents

    Args:
        file_path: Path to PDF file
        options: Dictionary with metadata and processing options

    Returns:
        Parsed document structure with:
        - documentId, fileName, mimeType, format, version
        - pages: List of page content with elements
        - fullText: Complete extracted text
        - metadata: Document metadata (language, subject, type, etc.)
        - processingInfo: Processing details and warnings
        - quizContent: Quiz-ready content (if requested)
        
    Raises:
        ValueError: If PDF parsing fails
    
    Example:
        >>> result = await parse_pdf(
        ...     Path("worksheet.pdf"),
        ...     {
        ...         "language": "en",
        ...         "subject": "math",
        ...         "document_type": "exercises",
        ...         "academic_level": "high",
        ...         "extract_tables": True,
        ...         "ocr_enabled": False
        ...     }
        ... )
    """
    # Delegate to the comprehensive PDF parser module
    return await _parse_pdf(file_path, options)