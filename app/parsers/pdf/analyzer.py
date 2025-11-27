"""
PDF Analyzer - Detects PDF characteristics and determines best parsing strategy
"""
from pathlib import Path
from typing import Dict, List, Tuple, Any
import logging
from dataclasses import dataclass
import hashlib
from functools import lru_cache

logger = logging.getLogger(__name__)


@lru_cache(maxsize=1)
def _get_fitz():
    import fitz
    return fitz


@lru_cache(maxsize=1)
def _get_pdfplumber():
    import pdfplumber
    return pdfplumber


@dataclass
class PDFCharacteristics:
    """Characteristics of a PDF document"""
    total_pages: int
    has_text: bool
    has_images: bool
    has_tables: bool
    has_forms: bool
    is_scanned: bool
    has_annotations: bool
    text_density: float  # Ratio of text to total content
    image_density: float  # Ratio of images to pages
    table_density: float  # Ratio of tables to pages
    avg_text_per_page: int
    fonts_used: List[str]
    page_sizes: List[Tuple[float, float]]
    has_equations: bool
    has_diagrams: bool
    recommended_strategy: str
    file_hash: str


def analyze_pdf(file_path: Path) -> PDFCharacteristics:
    """
    Analyze PDF to determine its characteristics

    Args:
        file_path: Path to PDF file

    Returns:
        PDFCharacteristics with analysis results
    """
    fitz = _get_fitz()

    # Calculate file hash for caching
    file_hash = _calculate_file_hash(file_path)

    try:
        doc = fitz.open(file_path)

        # Basic metadata
        total_pages = doc.page_count

        # Analyze content
        text_pages = 0
        image_count = 0
        total_text_length = 0
        fonts = set()
        page_sizes = []
        has_annotations = False
        potential_equations = False
        potential_diagrams = False

        # Sample pages for efficiency (analyze first 10, middle 5, last 5)
        pages_to_analyze = _get_sample_pages(total_pages)

        for page_num in pages_to_analyze:
            if page_num >= total_pages:
                continue

            page = doc[page_num]

            # Text analysis
            text = page.get_text()
            text_length = len(text.strip())
            total_text_length += text_length

            if text_length > 50:  # Meaningful text threshold
                text_pages += 1

            # Font analysis
            for font in page.get_fonts():
                fonts.add(font[3])  # Font name
                # Check for math fonts (indicators of equations)
                # Include Arabic fonts commonly used in educational materials
                math_and_arabic_fonts = ['math', 'symbol', 'equation', 'cmr', 'cmmi',
                                        'arabic', 'traditional arabic', 'simplified arabic',
                                        'arial unicode', 'tahoma', 'droid arabic']
                if any(font_name in font[3].lower() for font_name in math_and_arabic_fonts):
                    if 'math' in font[3].lower() or 'symbol' in font[3].lower():
                        potential_equations = True

            # Image analysis
            images = page.get_images()
            image_count += len(images)

            # Check for diagrams (images with specific characteristics)
            for img in images:
                xref = img[0]
                try:
                    pix = doc.extract_image(xref)
                    if pix:
                        # Large images are often diagrams
                        if pix['width'] > 200 and pix['height'] > 200:
                            potential_diagrams = True
                except Exception as e:
                    logger.debug("Failed to extract image %s: %s", xref, e)

            # Annotations
            if page.annots():
                has_annotations = True

            # Page size
            page_sizes.append((page.rect.width, page.rect.height))

        # Extrapolate for full document
        sample_size = len(pages_to_analyze)
        if sample_size > 0:
            text_pages = int((text_pages / sample_size) * total_pages)
            image_count = int((image_count / sample_size) * total_pages)
            total_text_length = int((total_text_length / sample_size) * total_pages)

        # Calculate densities
        avg_text_per_page = total_text_length // total_pages if total_pages > 0 else 0
        text_density = text_pages / total_pages if total_pages > 0 else 0
        image_density = image_count / total_pages if total_pages > 0 else 0

        # Determine if scanned
        is_scanned = _is_scanned_pdf(
            text_density=text_density,
            avg_text_per_page=avg_text_per_page,
            image_density=image_density
        )

        # Table detection (using pdfplumber for better accuracy)
        table_density = _detect_tables(file_path, total_pages)

        # Check for mathematical content
        if not potential_equations:
            potential_equations = _has_math_content(text)

        # Determine recommended strategy
        strategy = _determine_strategy(
            is_scanned=is_scanned,
            text_density=text_density,
            image_density=image_density,
            table_density=table_density,
            has_equations=potential_equations
        )

        doc.close()

        return PDFCharacteristics(
            total_pages=total_pages,
            has_text=text_density > 0.1,
            has_images=image_count > 0,
            has_tables=table_density > 0,
            has_forms=False,
            is_scanned=is_scanned,
            has_annotations=has_annotations,
            text_density=text_density,
            image_density=image_density,
            table_density=table_density,
            avg_text_per_page=avg_text_per_page,
            fonts_used=list(fonts),
            page_sizes=page_sizes,
            has_equations=potential_equations,
            has_diagrams=potential_diagrams,
            recommended_strategy=strategy,
            file_hash=file_hash
        )

    except Exception as e:
        logger.error("Failed to analyze PDF: %s", e)
        # Return default characteristics
        return PDFCharacteristics(
            total_pages=0,
            has_text=False,
            has_images=False,
            has_tables=False,
            has_forms=False,
            is_scanned=False,
            has_annotations=False,
            text_density=0,
            image_density=0,
            table_density=0,
            avg_text_per_page=0,
            fonts_used=[],
            page_sizes=[],
            has_equations=False,
            has_diagrams=False,
            recommended_strategy="text_focus",
            file_hash=file_hash
        )


def _calculate_file_hash(file_path: Path) -> str:
    """Calculate hash of file for caching"""
    hasher = hashlib.md5()
    with open(file_path, 'rb') as f:
        # Read in chunks for large files
        for chunk in iter(lambda: f.read(4096), b''):
            hasher.update(chunk)
    return hasher.hexdigest()


def _get_sample_pages(total_pages: int) -> List[int]:
    """Get sample pages for efficient analysis"""
    if total_pages <= 20:
        # For small documents, analyze all pages
        return list(range(total_pages))

    # For larger documents, sample strategically
    samples = []

    # First 10 pages
    samples.extend(range(min(10, total_pages)))

    # Middle 5 pages
    middle = total_pages // 2
    samples.extend(range(max(10, middle - 2), min(middle + 3, total_pages)))

    # Last 5 pages
    samples.extend(range(max(middle + 3, total_pages - 5), total_pages))

    return list(set(samples))  # Remove duplicates


def _is_scanned_pdf(text_density: float, avg_text_per_page: int, image_density: float) -> bool:
    """
    Determine if PDF is scanned based on heuristics

    Scanned PDFs typically have:
    - Very low text density (< 0.1)
    - High image density (> 0.8)
    - Very little extractable text per page (< 100 chars)
    """
    if text_density < 0.1 and avg_text_per_page < 100:
        return True
    if image_density > 0.8 and avg_text_per_page < 200:
        return True
    return False


def _detect_tables(file_path: Path, total_pages: int) -> float:
    """
    Detect tables in PDF and return table density
    """
    pdfplumber = _get_pdfplumber()

    try:
        pages_with_tables = 0

        with pdfplumber.open(file_path) as pdf:
            # Sample pages for efficiency
            pages_to_check = _get_sample_pages(min(total_pages, len(pdf.pages)))

            for page_num in pages_to_check[:20]:  # Limit to 20 pages for performance
                if page_num < len(pdf.pages):
                    page = pdf.pages[page_num]
                    tables = page.find_tables()
                    if tables:
                        pages_with_tables += 1

        # Estimate table density
        return pages_with_tables / len(pages_to_check) if pages_to_check else 0

    except Exception as e:
        logger.warning("Table detection failed: %s", e)
        return 0.0


def _has_math_content(text: str) -> bool:
    """Check if text contains mathematical content"""
    math_indicators = [
        '∑', '∫', '∂', '√', '±', '×', '÷', '≈', '≠', '≤', '≥',
        '∈', '∉', '⊂', '⊃', '∪', '∩', '∀', '∃', '∇', '∆',
        'α', 'β', 'γ', 'δ', 'θ', 'λ', 'μ', 'σ', 'π', 'φ', 'ω'
    ]

    # Check for mathematical symbols
    if any(indicator in text for indicator in math_indicators):
        return True

    # Check for equation patterns
    import re
    equation_patterns = [
        r'\b[a-z]\s*=\s*\d+',  # Simple equations like x = 5
        r'\d+\s*[+\-*/]\s*\d+\s*=',  # Arithmetic
        r'\\[a-z]+\{',  # LaTeX commands
        r'\$.*\$',  # LaTeX inline math
        # Arabic number patterns
        r'[\u0660-\u0669]+\s*[+\-*/]\s*[\u0660-\u0669]+',  # Arabic numerals arithmetic
        r'[٠-٩]+\s*[+\-*/]\s*[٠-٩]+',  # Alternative Arabic numerals
    ]

    for pattern in equation_patterns:
        if re.search(pattern, text):
            return True

    return False


def _determine_strategy(
    is_scanned: bool,
    text_density: float,
    image_density: float,
    table_density: float,
    has_equations: bool
) -> str:
    """
    Determine the best parsing strategy based on PDF characteristics

    Returns:
        Strategy name: 'ocr_heavy', 'hybrid', 'text_focus', 'table_focus', 'math_focus'
    """
    if is_scanned:
        return 'ocr_heavy'

    if has_equations:
        return 'math_focus'

    if table_density > 0.3:
        return 'table_focus'

    if image_density > 0.5 and text_density < 0.5:
        return 'hybrid'

    if text_density > 0.7:
        return 'text_focus'

    return 'hybrid'


def detect_content_regions(page) -> Dict[str, List[Dict[str, Any]]]:
    """
    Detect different content regions on a page

    Returns regions classified as:
    - text_blocks
    - image_regions
    - table_regions
    - equation_regions
    """
    regions = {
        'text_blocks': [],
        'image_regions': [],
        'table_regions': [],
        'equation_regions': []
    }

    # Get text blocks with positions
    blocks = page.get_text("dict")

    for block in blocks.get("blocks", []):
        bbox = block.get("bbox", [0, 0, 0, 0])

        if block.get("type") == 0:  # Text block
            text = ""
            for line in block.get("lines", []):
                for span in line.get("spans", []):
                    text += span.get("text", "")

            if text.strip():
                regions['text_blocks'].append({
                    'bbox': bbox,
                    'text': text,
                    'chars': len(text)
                })

                # Check if it might be an equation
                if _is_potential_equation(text):
                    regions['equation_regions'].append({
                        'bbox': bbox,
                        'text': text
                    })

        elif block.get("type") == 1:  # Image block
            regions['image_regions'].append({
                'bbox': bbox,
                'width': bbox[2] - bbox[0],
                'height': bbox[3] - bbox[1]
            })

    return regions


def _is_potential_equation(text: str) -> bool:
    """Check if text might be an equation"""
    equation_indicators = [
        '=', '∑', '∫', '∂', '√', '±', '×', '÷',
        'sin', 'cos', 'tan', 'log', 'exp', 'lim',
        '^', '_', 'α', 'β', 'γ', 'δ', 'θ', 'λ', 'μ', 'σ', 'π'
    ]

    return any(indicator in text for indicator in equation_indicators)