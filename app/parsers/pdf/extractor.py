"""
PDF Content Extractor - Async/sync implementation with multiple strategies
Supports text-focused, OCR-heavy, hybrid, table-focused, and math-focused extraction
"""
from pathlib import Path
from typing import Dict, Any, List
import logging
import asyncio
from concurrent.futures import ThreadPoolExecutor

logger = logging.getLogger(__name__)

# Import specialized extraction modules
from .table_extractor import extract_tables_from_pdf
from .math_extractor import extract_math_from_pdf, extract_equations_from_page
from .ocr_processor import process_pdf_with_ocr, process_page_with_ocr

# Thread pool for CPU-bound sync operations
executor = ThreadPoolExecutor(max_workers=4)

# Lazy imports
_pdfplumber = None
_fitz = None


def _get_pdfplumber():
    global _pdfplumber
    if _pdfplumber is None:
        import pdfplumber
        _pdfplumber = pdfplumber
    return _pdfplumber


def _get_fitz():
    global _fitz
    if _fitz is None:
        import fitz
        _fitz = fitz
    return _fitz


async def extract_with_strategy(
    file_path: Path,
    strategy: str,
    options: Dict[str, Any]
) -> Dict[str, Any]:
    """
    Extract content using the specified strategy
    
    Args:
        file_path: Path to PDF file
        strategy: Extraction strategy ('text_focus', 'ocr_heavy', 'hybrid', 'table_focus', 'math_focus')
        options: Extraction options (max_pages, extract_tables, extract_images, ocr_enabled, etc.)
    
    Returns:
        Extracted content with pages, metadata, and elements
    """
    logger.info("Extracting with strategy: %s", strategy)

    strategies = {
        'text_focus': extract_text_focused,
        'ocr_heavy': extract_ocr_heavy,
        'hybrid': extract_hybrid,
        'table_focus': extract_table_focused,
        'math_focus': extract_math_focused
    }

    extractor = strategies.get(strategy, extract_text_focused)
    
    # Log if using fallback strategy
    if strategy not in strategies:
        logger.warning("Unknown strategy '%s', falling back to 'text_focus'", strategy)
    
    return await extractor(file_path, options)


async def extract_text_focused(file_path: Path, options: Dict[str, Any]) -> Dict[str, Any]:
    """
    Fast text extraction using pdfplumber
    
    Best for: Text-heavy PDFs with minimal images/tables
    Performance: Fastest extraction method
    """
    loop = asyncio.get_event_loop()

    # Run sync pdfplumber operations in thread pool
    def _sync_extract():
        pdfplumber = _get_pdfplumber()
        pages = []
        full_text = []
        total_word_count = 0

        with pdfplumber.open(file_path) as pdf:
            max_pages = min(options.get("max_pages", len(pdf.pages)), len(pdf.pages))

            for i in range(max_pages):
                page = pdf.pages[i]
                text = page.extract_text() or ""
                word_count = len(text.split())

                page_data = {
                    "pageNumber": i + 1,
                    "content": text,
                    "metadata": {
                        "wordCount": word_count,
                        "characterCount": len(text),
                        "paragraphCount": text.count("\n\n") + 1 if text else 0,
                        "hasImages": False,
                        "hasTables": False,
                        "estimatedReadingTime": word_count // 200
                    },
                    "elements": {}
                }

                # Extract tables if requested
                if options.get("extract_tables", True):
                    tables = page.extract_tables()
                    if tables:
                        page_data["elements"]["tables"] = _format_simple_tables(tables)
                        page_data["metadata"]["hasTables"] = True

                pages.append(page_data)
                full_text.append(text)
                total_word_count += word_count

        return {
            "pages": pages,
            "fullText": "\n\n".join(full_text),
            "totalWordCount": total_word_count,
            "extractionMethod": "text_focused"
        }

    # Run in thread pool
    return await loop.run_in_executor(executor, _sync_extract)


async def extract_hybrid(file_path: Path, options: Dict[str, Any]) -> Dict[str, Any]:
    """
    Hybrid extraction combining native text + OCR where needed
    
    Best for: Mixed PDFs with both native text and scanned sections
    Performance: Balanced speed and quality
    """
    loop = asyncio.get_event_loop()

    # First pass: extract text synchronously
    def _sync_extract_text():
        fitz = _get_fitz()
        doc = fitz.open(file_path)
        max_pages = min(options.get("max_pages", doc.page_count), doc.page_count)

        pages_data = []
        for page_num in range(max_pages):
            page = doc[page_num]
            text = page.get_text()
            text_density = len(text.strip()) / (page.rect.width * page.rect.height) if page.rect.width > 0 else 0

            pages_data.append({
                "page": page,
                "page_num": page_num,
                "text": text,
                "text_density": text_density,
                "needs_ocr": text_density < 0.01 and options.get("ocr_enabled", False)
            })

        return pages_data, doc

    # Extract text in thread pool
    pages_data, doc = await loop.run_in_executor(executor, _sync_extract_text)

    # Process pages with OCR if needed (async)
    pages = []
    full_text = []
    total_word_count = 0

    for pd in pages_data:
        text = pd["text"]

        # Apply OCR if needed
        if pd["needs_ocr"]:
            ocr_text = await process_page_with_ocr(pd["page"], options)
            if ocr_text:
                text = f"{text}\n{ocr_text}".strip()

        word_count = len(text.split())

        page_data = {
            "pageNumber": pd["page_num"] + 1,
            "content": text,
            "metadata": {
                "wordCount": word_count,
                "characterCount": len(text),
                "textDensity": pd["text_density"],
                "hasImages": False,
                "hasTables": False,
                "ocrApplied": pd["needs_ocr"],
                "estimatedReadingTime": word_count // 200
            },
            "elements": {}
        }

        # Extract images if requested (sync operation)
        if options.get("extract_images", False):
            # Fix closure issue by capturing variables
            page_obj = pd["page"]
            page_number = pd["page_num"] + 1
            
            def _extract_imgs(p=page_obj, pn=page_number):
                return _extract_page_images(p, pn)

            images = await loop.run_in_executor(executor, _extract_imgs)
            if images:
                page_data["elements"]["images"] = images
                page_data["metadata"]["hasImages"] = True

        # Extract equations if math content detected
        if _has_math_content(text):
            # Fix closure issue by capturing variables
            page_obj = pd["page"]
            page_number = pd["page_num"] + 1
            
            def _extract_eqs(p=page_obj, pn=page_number, opts=options):
                return extract_equations_from_page(p, pn, opts)

            equations = await loop.run_in_executor(executor, _extract_eqs)
            if equations:
                page_data["elements"]["equations"] = equations

        pages.append(page_data)
        full_text.append(text)
        total_word_count += word_count

    doc.close()

    return {
        "pages": pages,
        "fullText": "\n\n".join(full_text),
        "totalWordCount": total_word_count,
        "extractionMethod": "hybrid"
    }


async def extract_table_focused(file_path: Path, options: Dict[str, Any]) -> Dict[str, Any]:
    """
    Table-focused extraction using pdfplumber's table detection
    
    Best for: PDFs with significant tabular data
    Performance: Slower but comprehensive table extraction
    """
    loop = asyncio.get_event_loop()

    # Run table extraction in thread pool
    tables = await loop.run_in_executor(
        executor,
        extract_tables_from_pdf,
        file_path,
        options
    )

    # Extract text content
    def _sync_extract_text():
        pdfplumber = _get_pdfplumber()
        pages = []
        full_text = []
        total_word_count = 0

        with pdfplumber.open(file_path) as pdf:
            for i, page in enumerate(pdf.pages):
                text = page.extract_text() or ""
                word_count = len(text.split())

                # Find tables for this page
                page_tables = [t for t in tables if t.get("page_number") == i + 1]

                page_data = {
                    "pageNumber": i + 1,
                    "content": text,
                    "metadata": {
                        "wordCount": word_count,
                        "characterCount": len(text),
                        "hasTables": len(page_tables) > 0,
                        "estimatedReadingTime": word_count // 200
                    },
                    "elements": {
                        "tables": page_tables
                    }
                }

                pages.append(page_data)
                full_text.append(text)
                total_word_count += word_count

        return pages, full_text, total_word_count

    pages, full_text, total_word_count = await loop.run_in_executor(executor, _sync_extract_text)

    return {
        "pages": pages,
        "fullText": "\n\n".join(full_text),
        "totalWordCount": total_word_count,
        "totalTables": len(tables),
        "extractionMethod": "table_focused"
    }


async def extract_math_focused(file_path: Path, options: Dict[str, Any]) -> Dict[str, Any]:
    """
    Math-focused extraction with equation detection
    
    Best for: Math/science PDFs with equations and formulas
    Performance: Specialized for mathematical content
    """
    loop = asyncio.get_event_loop()

    # Extract equations and text in parallel
    equations_task = loop.run_in_executor(
        executor,
        extract_math_from_pdf,
        file_path,
        options
    )

    def _sync_extract_text():
        fitz = _get_fitz()
        doc = fitz.open(file_path)

        pages = []
        full_text = []
        total_word_count = 0

        for page_num in range(doc.page_count):
            page = doc[page_num]
            text = page.get_text()
            word_count = len(text.split())

            pages.append({
                "pageNumber": page_num + 1,
                "content": text,
                "metadata": {
                    "wordCount": word_count,
                    "characterCount": len(text),
                    "estimatedReadingTime": word_count // 200
                },
                "elements": {}
            })

            full_text.append(text)
            total_word_count += word_count

        doc.close()
        return pages, full_text, total_word_count

    # Run both in parallel
    equations, (pages, full_text, total_word_count) = await asyncio.gather(
        equations_task,
        loop.run_in_executor(executor, _sync_extract_text)
    )

    # Merge equations into pages
    for page in pages:
        page_equations = [eq for eq in equations if eq.get("page_number") == page["pageNumber"]]
        if page_equations:
            page["elements"]["equations"] = page_equations
            page["metadata"]["hasEquations"] = True

    return {
        "pages": pages,
        "fullText": "\n\n".join(full_text),
        "totalWordCount": total_word_count,
        "totalEquations": len(equations),
        "equations": equations,
        "extractionMethod": "math_focused"
    }


async def extract_ocr_heavy(file_path: Path, options: Dict[str, Any]) -> Dict[str, Any]:
    """
    OCR-heavy extraction for scanned documents
    
    Best for: Fully scanned PDFs with minimal native text
    Performance: Slowest but handles image-based PDFs
    """
    return await process_pdf_with_ocr(file_path, options)


# Helper functions

def _format_simple_tables(tables: List) -> List[Dict[str, Any]]:
    """Simple table formatting"""
    formatted = []
    for i, table in enumerate(tables):
        if table and len(table) > 0:
            headers = table[0] if len(table) > 0 else []
            rows = table[1:] if len(table) > 1 else []

            formatted.append({
                "id": f"table_{i}",
                "headers": headers,
                "rows": rows,
                "representations": {
                    "markdown": _simple_table_to_markdown(headers, rows)
                }
            })
    return formatted


def _simple_table_to_markdown(headers: List, rows: List[List]) -> str:
    """Convert table to markdown"""
    if not headers and not rows:
        return ""

    lines = []
    if headers:
        lines.append("| " + " | ".join(str(h) for h in headers) + " |")
        lines.append("| " + " | ".join(["---"] * len(headers)) + " |")

    for row in rows:
        lines.append("| " + " | ".join(str(cell) if cell else "" for cell in row) + " |")

    return "\n".join(lines)


def _extract_page_images(page, page_num: int) -> List[Dict[str, Any]]:
    """Extract image information"""
    images = []
    image_list = page.get_images()

    for img_index, img in enumerate(image_list):
        xref = img[0]
        try:
            pix = page.parent.extract_image(xref)
            if pix:
                images.append({
                    "id": f"page{page_num}_img{img_index}",
                    "pageNumber": page_num,
                    "format": pix.get("ext", "unknown"),
                    "width": pix.get("width", 0),
                    "height": pix.get("height", 0),
                    "size": len(pix.get("image", b""))
                })
        except Exception as e:
            logger.debug("Failed to extract image %s from page %s: %s", img_index, page_num, e)

    return images


def _has_math_content(text: str) -> bool:
    """Check for mathematical content"""
    math_indicators = ['∑', '∫', '∂', '√', '±', '×', '÷', '≈', '≠', '≤', '≥', 'α', 'β', 'γ', 'δ', 'θ', 'π']
    return any(indicator in text for indicator in math_indicators)