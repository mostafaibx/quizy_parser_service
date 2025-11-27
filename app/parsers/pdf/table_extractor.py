"""
Table Extractor - Extracts and formats tables from PDFs
Converts tables to multiple formats: Markdown, CSV, HTML
"""
from pathlib import Path
from typing import Dict, Any, List, Optional
import logging
from functools import lru_cache

logger = logging.getLogger(__name__)

@lru_cache(maxsize=1)
def _get_pdfplumber():
    """
    Get pdfplumber module (lazy import)
    
    Returns:
        pdfplumber module
    """
    import pdfplumber
    return pdfplumber


def extract_tables_from_pdf(file_path: Path, options: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
    """
    Extract all tables from PDF using pdfplumber
    
    Processes tables and converts them to multiple formats (Markdown, CSV, HTML).
    Includes table analysis (data types, numeric detection).
    
    Args:
        file_path: Path to PDF file
        options: Extraction options (pages: 'all' or list of page numbers)
    
    Returns:
        List of table dictionaries with structure, representations, and analysis
    """
    options = options or {}
    pdfplumber = _get_pdfplumber()
    tables = []

    with pdfplumber.open(file_path) as pdf:
        pages_to_process = options.get("pages", "all")

        if pages_to_process == "all":
            pages = pdf.pages
        elif isinstance(pages_to_process, list):
            pages = [pdf.pages[i - 1] for i in pages_to_process if 0 < i <= len(pdf.pages)]
        else:
            pages = pdf.pages

        for page_num, page in enumerate(pages, 1):
            page_tables = page.extract_tables()

            for table_idx, raw_table in enumerate(page_tables):
                if raw_table:
                    processed = _process_table(raw_table, page_num, table_idx)
                    if processed:
                        tables.append(processed)

    return tables


def extract_tables_from_regions(page, regions: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Extract tables from specific regions on a page
    
    Args:
        page: pdfplumber page object
        regions: List of region dictionaries with 'bbox' coordinates
    
    Returns:
        List of processed table dictionaries
    """
    tables = []

    for idx, region in enumerate(regions):
        bbox = region.get("bbox", [])
        if len(bbox) == 4 and hasattr(page, 'within_bbox'):
            try:
                cropped = page.within_bbox(bbox)
                region_tables = cropped.extract_tables()

                if region_tables:
                    for table in region_tables:
                        processed = _process_table(table, page.page_number, idx)
                        if processed:
                            tables.append(processed)
            except Exception as e:
                logger.debug("Failed to extract table from region: %s", e)

    return tables


def _process_table(raw_table: List[List], page_num: int, table_idx: int) -> Optional[Dict[str, Any]]:
    """
    Process raw table data into structured format
    
    Args:
        raw_table: Raw table data from pdfplumber
        page_num: Page number (1-indexed)
        table_idx: Table index on the page
    
    Returns:
        Processed table dictionary with headers, rows, representations, and analysis
        Returns None if table is empty or invalid
    """
    if not raw_table:
        return None

    # Clean table data
    cleaned = []
    for row in raw_table:
        if row:
            cleaned_row = [str(cell).strip() if cell else '' for cell in row]
            if any(cleaned_row):
                cleaned.append(cleaned_row)

    if not cleaned:
        return None

    # Detect headers
    headers = []
    data_rows = cleaned

    if _is_header_row(cleaned[0]):
        headers = cleaned[0]
        data_rows = cleaned[1:] if len(cleaned) > 1 else []

    # Build table structure
    table = {
        "id": f"page{page_num}_table{table_idx}",
        "page_number": page_num,
        "headers": headers,
        "rows": data_rows,
        "num_rows": len(data_rows),
        "num_cols": len(headers) if headers else (len(data_rows[0]) if data_rows else 0)
    }

    # Add representations
    table["representations"] = {
        "markdown": _to_markdown(headers, data_rows),
        "csv": _to_csv(headers, data_rows),
        "html": _to_html(headers, data_rows)
    }

    # Add analysis
    table["analysis"] = _analyze_table(headers, data_rows)

    return table


def _is_header_row(row: List) -> bool:
    """
    Check if row is likely a header
    
    Heuristics:
    - At least 50% of cells are non-empty
    - Not all cells are numeric (headers are usually text)
    
    Args:
        row: Table row to check
    
    Returns:
        True if row is likely a header
    """
    if not row:
        return False

    # Headers typically have all non-empty values and contain text
    non_empty = sum(1 for cell in row if cell)
    if non_empty < len(row) * 0.5:
        return False

    # Check if all numeric (unlikely headers)
    numeric_count = sum(1 for cell in row if _is_numeric(cell))
    return numeric_count < len(row)


def _is_numeric(value: str) -> bool:
    """
    Check if value is numeric
    
    Strips common formatting characters ($, %, ,) before checking.
    
    Args:
        value: String value to check
    
    Returns:
        True if value can be converted to float
    """
    try:
        float(str(value).replace(',', '').replace('$', '').replace('%', ''))
        return True
    except (ValueError, AttributeError):
        return False


def _to_markdown(headers: List, rows: List[List]) -> str:
    """
    Convert table to Markdown format
    
    Args:
        headers: Table headers
        rows: Table data rows
    
    Returns:
        Markdown formatted table string
    """
    if not headers and not rows:
        return ""

    lines = []

    if headers:
        lines.append("| " + " | ".join(str(h) for h in headers) + " |")
        lines.append("| " + " | ".join(["---"] * len(headers)) + " |")

    for row in rows:
        lines.append("| " + " | ".join(str(cell) for cell in row) + " |")

    return "\n".join(lines)


def _to_csv(headers: List, rows: List[List]) -> str:
    """
    Convert table to CSV format
    
    Args:
        headers: Table headers
        rows: Table data rows
    
    Returns:
        CSV formatted table string
    """
    import csv
    import io

    output = io.StringIO()
    writer = csv.writer(output)

    if headers:
        writer.writerow(headers)
    writer.writerows(rows)

    return output.getvalue()


def _to_html(headers: List, rows: List[List]) -> str:
    """
    Convert table to HTML format
    
    Args:
        headers: Table headers
        rows: Table data rows
    
    Returns:
        HTML formatted table string
    """
    html = ["<table>"]

    if headers:
        html.append("  <thead><tr>")
        for h in headers:
            html.append(f"    <th>{h}</th>")
        html.append("  </tr></thead>")

    html.append("  <tbody>")
    for row in rows:
        html.append("  <tr>")
        for cell in row:
            html.append(f"    <td>{cell}</td>")
        html.append("  </tr>")
    html.append("  </tbody>")

    html.append("</table>")
    return "\n".join(html)


def _analyze_table(headers: List, rows: List[List]) -> Dict[str, Any]:
    """
    Analyze table structure and content
    
    Detects:
    - Data types per column (numeric vs text)
    - Presence of numeric data
    - Row and column counts
    
    Args:
        headers: Table headers
        rows: Table data rows
    
    Returns:
        Analysis dictionary with data types and statistics
    """
    analysis = {
        "data_types": [],
        "has_numeric": False,
        "row_count": len(rows),
        "col_count": len(headers) if headers else (len(rows[0]) if rows else 0)
    }

    if not rows:
        return analysis

    # Check data types per column
    for col_idx in range(analysis["col_count"]):
        col_values = [row[col_idx] if col_idx < len(row) else '' for row in rows]
        numeric_count = sum(1 for val in col_values if _is_numeric(val))

        if numeric_count > len(col_values) * 0.5:
            analysis["data_types"].append("numeric")
            analysis["has_numeric"] = True
        else:
            analysis["data_types"].append("text")

    return analysis