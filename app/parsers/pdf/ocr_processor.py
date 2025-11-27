"""
OCR Processor - Async OCR processing for scanned PDFs
Supports Google Cloud Vision API and Tesseract OCR with fallback mechanisms
"""
from pathlib import Path
from typing import Dict, Any
import logging
import io
import asyncio
from concurrent.futures import ThreadPoolExecutor
from PIL import Image
from functools import lru_cache

from app.utils.temp_manager import temp_directory, ensure_tmp_space

logger = logging.getLogger(__name__)

# Thread pool for sync operations
executor = ThreadPoolExecutor(max_workers=2)

# Lazy imports
@lru_cache(maxsize=1)
def _get_vision_client():
    """
    Get or create Google Cloud Vision client (singleton)
    
    Returns:
        ImageAnnotatorClient instance or None if unavailable
    """
    try:
        from google.cloud import vision
        client = vision.ImageAnnotatorClient()
        logger.info("Google Cloud Vision client initialized")
        return client
    except Exception as e:
        logger.debug("Google Cloud Vision not available: %s", e)
        return None


@lru_cache(maxsize=1)
def _get_pytesseract():
    """
    Get pytesseract module (lazy import)
    
    Returns:
        pytesseract module or None if unavailable
    """
    try:
        import pytesseract
        logger.info("Pytesseract initialized")
        return pytesseract
    except ImportError as e:
        logger.debug("pytesseract not available: %s", e)
        return None



@lru_cache(maxsize=1)
def _get_pdf2image():
    """
    Get pdf2image convert function (lazy import)
    
    Returns:
        convert_from_path function or None if unavailable
    """
    try:
        from pdf2image import convert_from_path
        logger.info("pdf2image initialized")
        return convert_from_path
    except ImportError as e:
        logger.debug("pdf2image not available: %s", e)
        return None


@lru_cache(maxsize=1)
def _get_fitz():
    """
    Get PyMuPDF (fitz) module (lazy import)
    
    Returns:
        fitz module or None if unavailable
    """
    try:
        import fitz
        logger.info("PyMuPDF (fitz) initialized")
        return fitz
    except ImportError as e:
        logger.debug("PyMuPDF not available: %s", e)
        return None



async def process_pdf_with_ocr(file_path: Path, options: Dict[str, Any]) -> Dict[str, Any]:
    """
    Process PDF with OCR using available methods
    
    Uses pdf2image for high-quality conversion, falls back to PyMuPDF.
    OCR processing uses Google Cloud Vision API with Tesseract fallback.
    
    Args:
        file_path: Path to PDF file
        options: Processing options (max_pages, language, preprocess, etc.)
    
    Returns:
        Extracted content with pages, metadata, and OCR method info
    """
    # Ensure enough temp space
    if not ensure_tmp_space(100):  # Need at least 100MB
        logger.error("Insufficient /tmp space for OCR processing")
        raise ValueError("Insufficient temporary storage space")

    loop = asyncio.get_event_loop()

    # Try pdf2image first for better quality
    convert_from_path = _get_pdf2image()
    if convert_from_path:
        try:
            # Use temp directory for pdf2image output
            with temp_directory(prefix="pdf2img_") as temp_dir:

                def _convert_pdf():
                    """Sync pdf2image conversion"""
                    return convert_from_path(
                        file_path,
                        dpi=200,
                        first_page=1,
                        last_page=options.get("max_pages"),
                        output_folder=temp_dir,
                        fmt='png'
                    )

                # Convert in thread pool
                images = await loop.run_in_executor(executor, _convert_pdf)

                # Process images concurrently
                tasks = []
                for page_num, img in enumerate(images, 1):
                    tasks.append(_process_page_image(img, page_num, options))

                # Process all pages concurrently (with limit)
                results = []
                for i in range(0, len(tasks), 5):  # Process 5 pages at a time
                    batch = tasks[i:i+5]
                    batch_results = await asyncio.gather(*batch)
                    results.extend(batch_results)

                # Build response
                pages = []
                full_text = []
                total_word_count = 0

                for page_data in results:
                    pages.append(page_data)
                    full_text.append(page_data["content"])
                    total_word_count += page_data["metadata"]["wordCount"]

                return {
                    "pages": pages,
                    "fullText": "\n\n".join(full_text),
                    "totalWordCount": total_word_count,
                    "extractionMethod": "ocr"
                }

        except Exception as e:
            logger.warning("pdf2image conversion failed: %s, falling back to PyMuPDF", e)
            return await _process_with_fitz(file_path, options)

    else:
        # Fallback to PyMuPDF
        return await _process_with_fitz(file_path, options)


async def _process_page_image(img: Image, page_num: int, options: Dict[str, Any]) -> Dict[str, Any]:
    """
    Process a single page image with OCR
    
    Args:
        img: PIL Image object
        page_num: Page number (1-indexed)
        options: OCR options
    
    Returns:
        Page data with OCR text and metadata
    """
    loop = asyncio.get_event_loop()

    # Convert PIL Image to bytes
    def _img_to_bytes():
        img_byte_arr = io.BytesIO()
        img.save(img_byte_arr, format='PNG')
        return img_byte_arr.getvalue()

    img_data = await loop.run_in_executor(executor, _img_to_bytes)

    # Process with OCR
    text = await _process_image_ocr(img_data, options)
    word_count = len(text.split())

    return {
        "pageNumber": page_num,
        "content": text,
        "metadata": {
            "wordCount": word_count,
            "characterCount": len(text),
            "ocrMethod": _get_ocr_method(),
            "estimatedReadingTime": word_count // 200
        },
        "elements": {}
    }


async def _process_with_fitz(file_path: Path, options: Dict[str, Any]) -> Dict[str, Any]:
    """
    Fallback PDF processing with PyMuPDF
    
    Uses PyMuPDF to render pages as images, then applies OCR.
    
    Args:
        file_path: Path to PDF file
        options: Processing options
    
    Returns:
        Extracted content with OCR text
    """
    loop = asyncio.get_event_loop()

    def _extract_pages():
        """Sync extraction of page images"""
        fitz = _get_fitz()
        if not fitz:
            raise ImportError("PyMuPDF (fitz) not available")

        doc = fitz.open(file_path)
        max_pages = min(options.get("max_pages", doc.page_count), doc.page_count)

        page_images = []
        for page_num in range(max_pages):
            page = doc[page_num]
            mat = fitz.Matrix(2, 2)  # 2x zoom
            pix = page.get_pixmap(matrix=mat)
            img_data = pix.tobytes("png")
            page_images.append((page_num + 1, img_data))

        doc.close()
        return page_images

    # Extract all page images
    page_images = await loop.run_in_executor(executor, _extract_pages)

    # Process pages concurrently
    tasks = []
    for page_num, img_data in page_images:
        tasks.append(_process_page_data(img_data, page_num, options))

    # Process with concurrency limit
    results = []
    for i in range(0, len(tasks), 5):  # 5 pages at a time
        batch = tasks[i:i+5]
        batch_results = await asyncio.gather(*batch)
        results.extend(batch_results)

    # Build response
    pages = []
    full_text = []
    total_word_count = 0

    for page_data in results:
        pages.append(page_data)
        full_text.append(page_data["content"])
        total_word_count += page_data["metadata"]["wordCount"]

    return {
        "pages": pages,
        "fullText": "\n\n".join(full_text),
        "totalWordCount": total_word_count,
        "extractionMethod": "ocr"
    }


async def _process_page_data(img_data: bytes, page_num: int, options: Dict[str, Any]) -> Dict[str, Any]:
    """
    Process page image data with OCR
    
    Args:
        img_data: PNG image bytes
        page_num: Page number (1-indexed)
        options: OCR options
    
    Returns:
        Page data with OCR text and metadata
    """
    text = await _process_image_ocr(img_data, options)
    word_count = len(text.split())

    return {
        "pageNumber": page_num,
        "content": text,
        "metadata": {
            "wordCount": word_count,
            "characterCount": len(text),
            "ocrMethod": _get_ocr_method(),
            "estimatedReadingTime": word_count // 200
        },
        "elements": {}
    }


async def process_page_with_ocr(page, options: Dict[str, Any]) -> str:
    """
    Process single PDF page with OCR
    
    Args:
        page: PyMuPDF page object
        options: OCR options
    
    Returns:
        Extracted text from OCR
    """
    loop = asyncio.get_event_loop()

    def _extract_image():
        fitz = _get_fitz()
        if not fitz:
            raise ImportError("PyMuPDF (fitz) not available")
        mat = fitz.Matrix(2, 2)
        pix = page.get_pixmap(matrix=mat)
        return pix.tobytes("png")

    img_data = await loop.run_in_executor(executor, _extract_image)
    return await _process_image_ocr(img_data, options)


async def _process_image_ocr(image_data: bytes, options: Dict[str, Any]) -> str:
    """
    Process image with OCR using available method
    
    Priority order:
    1. Google Cloud Vision API (best quality)
    2. Tesseract OCR (local fallback)
    
    Args:
        image_data: PNG image bytes
        options: OCR options (language, preprocess, etc.)
    
    Returns:
        Extracted text string
    """
    loop = asyncio.get_event_loop()

    # Try Google Cloud Vision first
    vision_client = _get_vision_client()
    if vision_client:
        try:
            def _vision_ocr():
                from google.cloud import vision
                # Create image object from bytes
                image = vision.Image(content=image_data)
                # Use document text detection for better results on scanned documents
                response = vision_client.document_text_detection(image=image)  # pylint: disable=no-member
                # Get full text from response
                if response.full_text_annotation:
                    return response.full_text_annotation.text
                elif response.text_annotations:
                    return response.text_annotations[0].description
                return ""

            result = await loop.run_in_executor(executor, _vision_ocr)
            if result:
                return result

        except Exception as e:
            logger.debug("Cloud Vision OCR failed: %s, trying fallback", e)

    # Try pytesseract as fallback
    pytesseract = _get_pytesseract()
    if pytesseract:
        try:
            def _tesseract_ocr():
                # Preprocess if requested
                img_data = image_data
                if options.get("preprocess", True):
                    img_data = _preprocess_image_sync(img_data)

                img = Image.open(io.BytesIO(img_data))
                config = '--psm 6'  # Uniform block of text
                lang = options.get("language", "eng")

                return pytesseract.image_to_string(img, lang=lang, config=config).strip()

            result = await loop.run_in_executor(executor, _tesseract_ocr)
            if result:
                return result

        except Exception as e:
            logger.warning("Pytesseract OCR failed: %s", e)

    # If all OCR methods fail
    logger.warning("No OCR service available")
    return ""


def _get_ocr_method() -> str:
    """
    Get the available OCR method
    
    Returns:
        'cloud_vision', 'tesseract', or 'none'
    """
    if _get_vision_client():
        return "cloud_vision"
    elif _get_pytesseract():
        return "tesseract"
    else:
        return "none"


def _preprocess_image_sync(image_data: bytes) -> bytes:
    """
    Synchronous image preprocessing for better OCR accuracy
    
    Applies:
    - Grayscale conversion
    - Contrast enhancement (1.5x)
    - Sharpness enhancement (2.0x)
    
    Args:
        image_data: Original image bytes
    
    Returns:
        Preprocessed image bytes (or original if preprocessing fails)
    """
    try:
        img = Image.open(io.BytesIO(image_data))

        # Convert to grayscale for better OCR
        if img.mode != 'L':
            img = img.convert('L')

        # Enhance contrast and sharpness
        from PIL import ImageEnhance
        enhancer = ImageEnhance.Contrast(img)
        img = enhancer.enhance(1.5)

        sharpness = ImageEnhance.Sharpness(img)
        img = sharpness.enhance(2.0)

        # Save to bytes
        output = io.BytesIO()
        img.save(output, format='PNG')
        return output.getvalue()

    except Exception as e:
        logger.error("Image preprocessing failed: %s", e)
        return image_data