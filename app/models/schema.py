"""
Pydantic models for request/response validation
Follows Pydantic v2 best practices with proper type hints and aliases
"""
from typing import Dict, Any, List, Optional, Literal
from pydantic import BaseModel, Field, ConfigDict, field_validator, HttpUrl
from datetime import datetime


# Request Models
class ParseRequest(BaseModel):
    """Request model for document parsing"""
    model_config = ConfigDict(
        extra="forbid",
        str_strip_whitespace=True,
        validate_assignment=True
    )

    file_id: str = Field(
        ..., 
        min_length=1,
        max_length=500,
        description="Unique file identifier"
    )
    file_url: HttpUrl = Field(
        ..., 
        description="Pre-signed URL to download file"
    )
    mime_type: str = Field(
        ..., 
        description="MIME type of the file"
    )
    options: Dict[str, Any] = Field(
        default_factory=dict, 
        description="Parsing options (extract_tables, ocr_enabled, etc.)"
    )

    @field_validator("mime_type")
    @classmethod
    def validate_mime_type(cls, v: str) -> str:
        """Validate MIME type is supported"""
        allowed = {
            "application/pdf",
            "text/plain",
            "application/msword",
            "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
            "text/csv",
            "text/html"
        }
        if v not in allowed:
            raise ValueError(
                f"Unsupported MIME type: {v}. "
                f"Allowed types: {', '.join(allowed)}"
            )
        return v


class ParseOptions(BaseModel):
    """Options for document parsing configuration"""
    model_config = ConfigDict(extra="allow")

    extract_images: bool = Field(
        default=False, 
        description="Extract and include images from document"
    )
    extract_tables: bool = Field(
        default=True, 
        description="Extract and parse tables"
    )
    extract_metadata: bool = Field(
        default=True, 
        description="Extract document metadata"
    )
    ocr_enabled: bool = Field(
        default=False, 
        description="Enable OCR for scanned documents"
    )
    ai_description_enabled: bool = Field(
        default=False, 
        description="Enable AI-generated descriptions"
    )
    max_pages: Optional[int] = Field(
        default=None,
        ge=1,
        le=1000,
        description="Maximum number of pages to process"
    )


# Response Models (using aliases for JSON compatibility)
class PageMetadata(BaseModel):
    """Metadata for a single page"""
    model_config = ConfigDict(populate_by_name=True)

    word_count: int = Field(
        ..., 
        alias="wordCount",
        description="Number of words on the page",
        ge=0
    )
    character_count: int = Field(
        ..., 
        alias="characterCount",
        description="Number of characters on the page",
        ge=0
    )
    paragraph_count: int = Field(
        default=0, 
        alias="paragraphCount",
        description="Number of paragraphs on the page",
        ge=0
    )
    has_images: bool = Field(
        default=False, 
        alias="hasImages",
        description="Whether the page contains images"
    )
    has_tables: bool = Field(
        default=False, 
        alias="hasTables",
        description="Whether the page contains tables"
    )
    estimated_reading_time: int = Field(
        default=0, 
        alias="estimatedReadingTime",
        description="Estimated reading time in seconds",
        ge=0
    )


class PageElements(BaseModel):
    """Structured elements extracted from a page"""
    model_config = ConfigDict(populate_by_name=True)

    headings: List[Dict[str, Any]] = Field(
        default_factory=list,
        description="Headings found on the page"
    )
    paragraphs: List[Dict[str, Any]] = Field(
        default_factory=list,
        description="Paragraphs found on the page"
    )
    lists: List[Dict[str, Any]] = Field(
        default_factory=list,
        description="Lists (ordered/unordered) found on the page"
    )
    images: List[Dict[str, Any]] = Field(
        default_factory=list,
        description="Images found on the page"
    )
    tables: List[Dict[str, Any]] = Field(
        default_factory=list,
        description="Tables found on the page"
    )
    footnotes: List[str] = Field(
        default_factory=list,
        description="Footnotes found on the page"
    )
    quotes: List[Dict[str, Any]] = Field(
        default_factory=list,
        description="Block quotes found on the page"
    )


class Page(BaseModel):
    """Single page data with content and metadata"""
    model_config = ConfigDict(populate_by_name=True)

    page_number: int = Field(
        ..., 
        alias="pageNumber",
        description="Page number (1-indexed)",
        ge=1
    )
    content: str = Field(
        ..., 
        description="Full text content of the page"
    )
    metadata: PageMetadata = Field(
        ..., 
        description="Page metadata and statistics"
    )
    elements: PageElements = Field(
        default_factory=PageElements,
        description="Structured elements extracted from the page"
    )


class DocumentMetadata(BaseModel):
    """Complete document metadata and statistics"""
    model_config = ConfigDict(populate_by_name=True)

    title: Optional[str] = Field(
        default=None,
        description="Document title"
    )
    author: Optional[str] = Field(
        default=None,
        description="Document author"
    )
    subject: Optional[str] = Field(
        default=None,
        description="Document subject"
    )
    keywords: List[str] = Field(
        default_factory=list,
        description="Document keywords/tags"
    )
    language: Optional[str] = Field(
        default=None,
        description="Document language (ISO 639-1 code)"
    )
    total_pages: int = Field(
        ..., 
        alias="totalPages",
        description="Total number of pages in document",
        ge=1
    )
    total_word_count: int = Field(
        ..., 
        alias="totalWordCount",
        description="Total word count across all pages",
        ge=0
    )
    estimated_total_reading_time: int = Field(
        ..., 
        alias="estimatedTotalReadingTime",
        description="Estimated reading time in seconds",
        ge=0
    )
    document_type: Optional[Literal["academic", "business", "technical", "general", "educational"]] = Field(
        default="general",
        alias="documentType",
        description="Classified document type"
    )
    academic_level: Optional[Literal["elementary", "middle", "high", "undergraduate", "graduate"]] = Field(
        default=None,
        alias="academicLevel",
        description="Academic level if applicable"
    )


class ProcessingInfo(BaseModel):
    """Processing metadata and statistics"""
    model_config = ConfigDict(populate_by_name=True)

    parsed_at: str = Field(
        default_factory=lambda: datetime.utcnow().isoformat(),
        alias="parsedAt",
        description="ISO 8601 timestamp of when document was parsed"
    )
    parser_version: str = Field(
        default="1.0.0",
        alias="parserVersion",
        description="Version of the parser used"
    )
    extraction_method: str = Field(
        ..., 
        alias="extractionMethod",
        description="Method used for text extraction (text/ocr/hybrid)"
    )
    processing_time: int = Field(
        ..., 
        alias="processingTime",
        description="Processing time in milliseconds",
        ge=0
    )
    warnings: List[str] = Field(
        default_factory=list,
        description="Any warnings encountered during processing"
    )


class ParsedDocument(BaseModel):
    """Complete parsed document structure"""
    model_config = ConfigDict(populate_by_name=True)

    document_id: str = Field(
        ..., 
        alias="documentId",
        description="Unique document identifier"
    )
    file_name: str = Field(
        ..., 
        alias="fileName",
        description="Original filename"
    )
    mime_type: str = Field(
        ..., 
        alias="mimeType",
        description="Document MIME type"
    )
    format: Literal["pdf", "docx", "doc", "txt", "rtf", "odt"] = Field(
        ..., 
        description="Document format"
    )
    version: str = Field(
        default="1.0",
        description="Schema version"
    )

    pages: List[Page] = Field(
        ..., 
        description="Array of parsed pages"
    )
    full_text: str = Field(
        ..., 
        alias="fullText",
        description="Full document text (all pages concatenated)"
    )

    metadata: DocumentMetadata = Field(
        ..., 
        description="Document metadata and statistics"
    )
    processing_info: ProcessingInfo = Field(
        ..., 
        alias="processingInfo",
        description="Processing metadata"
    )

    # Optional quiz content (can be added by enhancers)
    quiz_content: Optional[Dict[str, Any]] = Field(
        default=None,
        alias="quizContent",
        description="Generated quiz content (if applicable)"
    )


class ParseResponse(BaseModel):
    """API response for parsing endpoint"""
    model_config = ConfigDict(populate_by_name=True)

    success: bool = Field(
        ..., 
        description="Whether the parsing was successful"
    )
    data: Optional[Dict[str, Any]] = Field(
        default=None,
        description="Parsed document data (follows ParsedDocument schema)"
    )
    processing_metrics: Optional[Dict[str, Any]] = Field(
        default=None,
        description="Processing metrics (duration, cache hit, etc.)"
    )
    error: Optional[Dict[str, Any]] = Field(
        default=None,
        description="Error details if success=false"
    )

    @field_validator("error")
    @classmethod
    def validate_error_structure(cls, v: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
        """Ensure error has required fields if present"""
        if v is not None:
            if "code" not in v or "message" not in v:
                raise ValueError("Error must contain 'code' and 'message' fields")
        return v


class HealthResponse(BaseModel):
    """Health check response"""
    model_config = ConfigDict(populate_by_name=True)

    status: Literal["healthy", "degraded", "unhealthy"] = Field(
        ..., 
        description="Service health status"
    )
    version: str = Field(
        ..., 
        description="Service version"
    )
    parsers: Dict[str, str] = Field(
        ..., 
        description="Available parsers and their status"
    )
    timestamp: str = Field(
        default_factory=lambda: datetime.utcnow().isoformat(),
        description="ISO 8601 timestamp"
    )


class ErrorResponse(BaseModel):
    """Standard error response"""
    model_config = ConfigDict(populate_by_name=True)

    success: bool = Field(
        default=False, 
        description="Always false for error responses"
    )
    error: Dict[str, Any] = Field(
        ..., 
        description="Error details with code and message"
    )

    @field_validator("error")
    @classmethod
    def validate_error_fields(cls, v: Dict[str, Any]) -> Dict[str, Any]:
        """Ensure error has required structure"""
        required_fields = {"code", "message"}
        missing = required_fields - set(v.keys())
        if missing:
            raise ValueError(f"Error must contain fields: {missing}")
        return v