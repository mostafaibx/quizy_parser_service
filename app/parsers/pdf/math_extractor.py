"""
Math Extractor - Extracts mathematical equations from PDFs
Supports LaTeX, inline math, display equations, and math symbols
"""
from pathlib import Path
from typing import Dict, Any, List, Optional
import re
import logging
from functools import lru_cache

logger = logging.getLogger(__name__)

# Lazy import
@lru_cache(maxsize=1)
def _get_fitz():
    import fitz
    return fitz


def extract_math_from_pdf(file_path: Path, options: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
    """
    Extract mathematical equations from PDF
    
    Supports:
    - LaTeX equations ($...$, $$...$$, \begin{equation}...\end{equation})
    - Inline math formulas
    - Display equations
    - Math symbols (∑, ∫, √, π, etc.)
    - Simple equations (x = 5, y = 2x + 3)
    
    Args:
        file_path: Path to PDF file
        options: Extraction options (max_pages, etc.)
    
    Returns:
        List of equation dictionaries with id, page_number, type, raw_text, and latex
    """
    options = options or {}
    fitz = _get_fitz()
    equations = []

    try:
        doc = fitz.open(file_path)
        max_pages = min(options.get("max_pages", doc.page_count), doc.page_count)

        for page_num in range(max_pages):
            page = doc[page_num]
            page_equations = extract_equations_from_page(page, page_num + 1)
            equations.extend(page_equations)

        doc.close()

    except Exception as e:
        logger.error("Failed to extract math from PDF: %s", e)

    return equations


def extract_equations_from_page(page, page_num: int) -> List[Dict[str, Any]]:
    """
    Extract equations from a single PDF page
    
    Args:
        page: PyMuPDF page object
        page_num: Page number (1-indexed)
    
    Returns:
        List of equation dictionaries
    """
    equations = []
    text = page.get_text()

    # Pattern-based extraction
    patterns = [
        (r'\$\$([^$]+)\$\$', 'display'),
        (r'\$([^$]+)\$', 'inline'),
        (r'\\begin{equation}(.*?)\\end{equation}', 'display'),
        (r'\\\[(.*?)\\\]', 'display'),
        (r'\\\((.*?)\\\)', 'inline'),
        (r'([a-zA-Z_]\w*\s*=\s*[^.,;]+)(?:[.,;]|\s|$)', 'simple'),
    ]

    eq_id = 0
    for pattern, eq_type in patterns:
        for match in re.finditer(pattern, text, re.MULTILINE | re.DOTALL):
            eq_text = match.group(1).strip()

            if len(eq_text) > 2 and _is_valid_equation(eq_text):
                equations.append({
                    "id": f"page{page_num}_eq{eq_id}",
                    "page_number": page_num,
                    "type": eq_type,
                    "raw_text": eq_text,
                    "latex": _convert_to_latex(eq_text, eq_type)
                })
                eq_id += 1

    # Check for math symbols in text blocks
    blocks = page.get_text("dict")
    for block in blocks.get("blocks", []):
        if block.get("type") == 0:  # Text block
            block_text = ""
            has_math_font = False

            for line in block.get("lines", []):
                for span in line.get("spans", []):
                    font = span.get("font", "").lower()
                    if any(mf in font for mf in ['math', 'symbol', 'cmr', 'cmmi']):
                        has_math_font = True
                    block_text += span.get("text", "")

            if has_math_font and block_text not in [eq["raw_text"] for eq in equations]:
                equations.append({
                    "id": f"page{page_num}_eq{eq_id}",
                    "page_number": page_num,
                    "type": "structural",
                    "raw_text": block_text.strip(),
                    "latex": _convert_to_latex(block_text.strip(), "structural")
                })
                eq_id += 1

    return equations


def _is_valid_equation(text: str) -> bool:
    """
    Check if text is likely a mathematical equation
    
    Validates that the text contains:
    - Mathematical operators (=, +, -, *, /, ^, _)
    - Alphanumeric characters (variables or numbers)
    """
    return any(op in text for op in ['=', '+', '-', '*', '/', '^', '_']) and \
           any(c.isalnum() for c in text)


def _convert_to_latex(text: str, eq_type: str) -> str:
    """
    Convert equation text to LaTeX format
    
    Performs:
    - Symbol conversion (×, ÷, ≈, etc. → LaTeX commands)
    - Fraction detection (a/b → \frac{a}{b})
    - Subscript/superscript formatting
    - Greek letter conversion (α, β, π, etc.)
    - Delimiter addition ($...$ or $$...$$)
    
    Args:
        text: Raw equation text
        eq_type: Equation type ('display', 'inline', 'simple', 'structural')
    
    Returns:
        LaTeX-formatted equation string
    """
    # Already in LaTeX format
    if eq_type in ['display', 'inline']:
        return text

    latex = text.strip()

    # Symbol replacements for common mathematical operators and Greek letters
    replacements = {
        # Operators
        '×': r'\times', '÷': r'\div', '±': r'\pm',
        '≈': r'\approx', '≠': r'\neq', '≤': r'\leq', '≥': r'\geq',
        '∞': r'\infty', '√': r'\sqrt', '∑': r'\sum', '∫': r'\int',
        # Greek letters
        'α': r'\alpha', 'β': r'\beta', 'γ': r'\gamma', 'δ': r'\delta',
        'θ': r'\theta', 'π': r'\pi', 'σ': r'\sigma', 'λ': r'\lambda',
        'μ': r'\mu', 'φ': r'\phi', 'ω': r'\omega',
    }

    for old, new in replacements.items():
        latex = latex.replace(old, new)

    # Handle fractions (e.g., a/b → \frac{a}{b})
    latex = re.sub(r'(\w+)/(\w+)', r'\\frac{\1}{\2}', latex)

    # Handle subscripts (e.g., x_2 → x_{2})
    latex = re.sub(r'(\w)_(\w+)', r'\1_{\2}', latex)
    
    # Handle superscripts (e.g., x^2 → x^{2})
    latex = re.sub(r'(\w)\^(\w+)', r'\1^{\2}', latex)

    # Add LaTeX delimiters if not present
    if not latex.startswith('$'):
        if eq_type == 'display' or len(latex) > 50:
            latex = f'$${latex}$$'  # Display mode for long equations
        else:
            latex = f'${latex}$'  # Inline mode for short equations

    return latex