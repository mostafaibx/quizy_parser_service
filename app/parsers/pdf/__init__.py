"""
Comprehensive PDF Parser Module
Handles all types of PDFs with multiple strategies
"""
from pathlib import Path
from typing import Dict, Any, Optional
import logging
import time
from app.utils.cache import get_from_cache, set_in_cache, get_cache_key

from .analyzer import analyze_pdf, PDFCharacteristics
from .extractor import extract_with_strategy
from .metadata_extractor import extract_pdf_metadata
from .content_analyzer import (
    extract_key_topics,
    generate_summary_points,
    identify_question_areas,
)

logger = logging.getLogger(__name__)


async def parse_pdf(
    file_path: Path,
    options: Optional[Dict[str, Any]] = None
) -> Dict[str, Any]:
    """
    Main entry point for PDF parsing
    Analyzes PDF and applies appropriate parsing strategy

    Args:
        file_path: Path to PDF file
        options: Parsing options including:
            - extract_images: bool
            - extract_tables: bool
            - ocr_enabled: bool
            - visual_processing: 'ocr' or 'ai'
            - max_pages: int
            - language: str (for OCR)
            - vision_provider: 'openai' or 'anthropic'
            - vision_api_key: str

    Returns:
        ParsedDocument structure as defined in schema
    """
    start_time = time.time()
    options = options or {}

    try:
        # Check cache first
        cache_key = get_cache_key(str(file_path), options)
        cached_result = get_from_cache(cache_key)
        if cached_result:
            logger.info("Returning cached result for %s", file_path.name)
            cached_result["processingInfo"]["cached"] = True
            return cached_result

        # Step 1: Analyze PDF characteristics
        logger.info("Analyzing PDF: %s", file_path.name)
        characteristics = analyze_pdf(file_path)

        # Log analysis results
        logger.info(
            "PDF Analysis: %d pages, strategy: %s, scanned: %s, text density: %.2f",
            characteristics.total_pages,
            characteristics.recommended_strategy,
            characteristics.is_scanned,
            characteristics.text_density
        )

        # Step 2: Apply extraction strategy
        strategy = options.get("force_strategy", characteristics.recommended_strategy)
        logger.info("Using extraction strategy: %s", strategy)

        result = await extract_with_strategy(file_path, strategy, options)

        # Step 3: Build final document structure
        parsed_document = _build_document_structure(
            file_path,
            result,
            characteristics,
            options,
            time.time() - start_time
        )

        # Cache successful result
        set_in_cache(cache_key, parsed_document, ttl=1800)  # 30 minutes

        return parsed_document

    except Exception as e:
        logger.error("PDF parsing failed for %s: %s", file_path.name, str(e))
        raise ValueError(f"Failed to parse PDF: {str(e)}") from e


def _build_document_structure(
    file_path: Path,
    extraction_result: Dict[str, Any],
    characteristics: PDFCharacteristics,
    options: Dict[str, Any],
    processing_time: float
) -> Dict[str, Any]:
    """
    Build the final document structure matching the schema
    
    Args:
        file_path: Path to PDF file
        extraction_result: Results from extraction strategy
        characteristics: PDF characteristics from analysis
        options: Parsing options with metadata
        processing_time: Time taken for processing in seconds
    
    Returns:
        Complete document structure with metadata, pages, and processing info
    """
    # Determine document format
    file_format = "pdf"

    # Extract PDF metadata using the new module
    pdf_metadata = extract_pdf_metadata(file_path)

    # Build metadata using client-provided values
    metadata = _build_metadata_with_fallback(
        pdf_metadata=pdf_metadata,
        characteristics=characteristics,
        extraction_result=extraction_result,
        options=options
    )

    # Build processing info - fix the undefined 'strategy' variable
    processing_info = {
        "parsedAt": time.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "parserVersion": "1.0.0",
        "extractionMethod": extraction_result.get("extractionMethod", characteristics.recommended_strategy),
        "processingTime": int(processing_time * 1000),  # Convert to milliseconds
        "warnings": []
    }

    # Add warnings if applicable
    if characteristics.is_scanned and not options.get("ocr_enabled"):
        processing_info["warnings"].append("Document appears to be scanned but OCR was not enabled")
    if extraction_result.get("totalWordCount", 0) < 100:
        processing_info["warnings"].append("Very little text extracted")

    # Build quiz content if we have enhanced visuals
    quiz_content = None
    if options.get("generate_quiz_content"):
        language = options.get("language", "en")
        quiz_content = _generate_quiz_content(extraction_result, language)

    return {
        "documentId": file_path.stem,
        "fileName": file_path.name,
        "mimeType": "application/pdf",
        "format": file_format,
        "version": "1.0",
        "pages": extraction_result.get("pages", []),
        "fullText": extraction_result.get("fullText", ""),
        "metadata": metadata,
        "processingInfo": processing_info,
        "quizContent": quiz_content
    }


def _build_metadata_with_fallback(
    pdf_metadata: Dict[str, Any],
    characteristics: PDFCharacteristics,
    extraction_result: Dict[str, Any],
    options: Dict[str, Any]
) -> Dict[str, Any]:
    """
    Build metadata using client-provided values (metadata-driven approach)
    
    Required fields from client (via options):
    - language: Document language code (en, ar, de, fr, es, it)
    - subject: Academic subject
    - document_type: Type of document (explanation, exercises, mixed)
    
    Optional fields:
    - academic_level: Educational level (primary, secondary, high, college)
    
    Args:
        pdf_metadata: Extracted PDF metadata (title, author, etc.)
        characteristics: PDF structural characteristics
        extraction_result: Text extraction results
        options: Client-provided parsing options with metadata
    
    Returns:
        Complete metadata dictionary
    """
    # All required fields from client request
    language = options.get("language", "en")
    logger.info("Using language: %s", language)
    
    subject = options.get("subject", "general")
    logger.info("Using subject: %s", subject)
    
    document_type = options.get("document_type", "mixed")
    logger.info("Using document type: %s", document_type)
    
    # Optional: Academic level from request
    academic_level = options.get("academic_level")
    
    return {
        "title": pdf_metadata.get("title"),
        "author": pdf_metadata.get("author"),
        "subject": subject or pdf_metadata.get("subject"),
        "keywords": pdf_metadata.get("keywords", []),
        "language": language,
        "totalPages": characteristics.total_pages,
        "totalWordCount": extraction_result.get("totalWordCount", 0),
        "estimatedTotalReadingTime": extraction_result.get("totalWordCount", 0) // 200,
        "documentType": document_type,
        "academicLevel": academic_level
    }


def _generate_quiz_content(extraction_result: Dict[str, Any], language: str = "en") -> Dict[str, Any]:
    """
    Generate quiz-ready content from extraction results
    
    Args:
        extraction_result: Results from PDF extraction
        language: Document language code (en, ar, de, fr, es, it)
    
    Returns:
        Quiz-ready content with topics and summaries
    """
    quiz_pages = []
    visual_descriptions = []

    pages = extraction_result.get("pages", [])

    for page in pages:
        # Build enhanced page content
        enhanced_content = page.get("content", "")

        # Add visual descriptions inline
        if "images" in page.get("elements", {}):
            for img in page["elements"]["images"]:
                if "ai_analysis" in img:
                    description = img["ai_analysis"].get("description", "")
                    if description:
                        enhanced_content += f"\n[Image: {description}]\n"
                        visual_descriptions.append({
                            "id": img.get("id"),
                            "type": "image",
                            "description": description,
                            "context": img.get("context", ""),
                            "possibleQuestions": []  # Will be generated by AI if enabled
                        })

        # Add table descriptions
        if "tables" in page.get("elements", {}):
            for table in page["elements"]["tables"]:
                if "description" in table:
                    enhanced_content += f"\n[Table: {table['description']}]\n"

        quiz_pages.append({
            "pageNumber": page["pageNumber"],
            "enhancedContent": enhanced_content,
            "elements": page.get("elements", {})
        })

    # Extract key topics using the new content analyzer module with language support
    full_text = extraction_result.get("fullText", "")
    key_topics = extract_key_topics(full_text, language=language)

    # Generate summary points using the new module
    summary_points = generate_summary_points(pages, key_topics, max_points=10)

    return {
        "pages": quiz_pages,
        "visualDescriptions": visual_descriptions,
        "keyTopics": key_topics,
        "summaryPoints": summary_points
    }


# Export main function
__all__ = ["parse_pdf"]