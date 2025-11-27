"""
Lazy import utilities for optimal cold start performance
Imports expensive libraries only when needed
"""
import logging
from typing import Any

logger = logging.getLogger(__name__)

# Global references for lazy-loaded modules
_modules = {}


def lazy_import(module_name: str) -> Any:
    """
    Lazy import a module

    Args:
        module_name: Name of the module to import

    Returns:
        Imported module

    Example:
        pdfplumber = lazy_import('pdfplumber')
    """
    if module_name not in _modules:
        logger.debug(f"Lazy loading module: {module_name}")
        _modules[module_name] = __import__(module_name)
    return _modules[module_name]


# Specific lazy loaders for heavy libraries
def get_pdfplumber():
    """Lazy load pdfplumber"""
    if 'pdfplumber' not in _modules:
        import pdfplumber
        _modules['pdfplumber'] = pdfplumber
    return _modules['pdfplumber']


def get_pymupdf():
    """Lazy load PyMuPDF"""
    if 'fitz' not in _modules:
        import fitz
        _modules['fitz'] = fitz
    return _modules['fitz']


def get_docx():
    """Lazy load python-docx"""
    if 'docx' not in _modules:
        import docx
        _modules['docx'] = docx
    return _modules['docx']


def get_pytesseract():
    """Lazy load pytesseract for OCR"""
    if 'pytesseract' not in _modules:
        import pytesseract
        _modules['pytesseract'] = pytesseract
    return _modules['pytesseract']


def get_pil():
    """Lazy load PIL/Pillow"""
    if 'PIL' not in _modules:
        from PIL import Image
        _modules['PIL'] = Image
    return _modules['PIL']


def get_pandas():
    """Lazy load pandas for CSV processing"""
    if 'pandas' not in _modules:
        import pandas
        _modules['pandas'] = pandas
    return _modules['pandas']


def get_bs4():
    """Lazy load BeautifulSoup for HTML parsing"""
    if 'bs4' not in _modules:
        from bs4 import BeautifulSoup
        _modules['bs4'] = BeautifulSoup
    return _modules['bs4']


def get_tabula():
    """Lazy load tabula for table extraction"""
    if 'tabula' not in _modules:
        import tabula
        _modules['tabula'] = tabula
    return _modules['tabula']


def get_camelot():
    """Lazy load camelot for advanced table extraction"""
    if 'camelot' not in _modules:
        import camelot
        _modules['camelot'] = camelot
    return _modules['camelot']


def clear_module_cache():
    """Clear all cached modules (useful for testing)"""
    global _modules
    _modules.clear()
    logger.info("Cleared module cache")