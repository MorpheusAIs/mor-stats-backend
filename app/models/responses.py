"""
Standardized API response models.
"""
from datetime import datetime
from typing import Any, Dict, Generic, List, Optional, TypeVar, Union

from pydantic import BaseModel, Field

# Type variable for the data field in responses
T = TypeVar('T')


class ErrorDetail(BaseModel):
    """Model for error details."""
    message: str = Field(..., description="Error message")
    code: Optional[str] = Field(None, description="Error code")
    field: Optional[str] = Field(None, description="Field with error")


class ErrorResponse(BaseModel):
    """Standard error response model."""
    error: str = Field(..., description="Error message")
    status_code: int = Field(..., description="HTTP status code")
    details: Optional[Dict[str, Any]] = Field(None, description="Additional error details")
    errors: Optional[List[ErrorDetail]] = Field(None, description="List of validation errors")
    timestamp: datetime = Field(default_factory=datetime.now, description="Error timestamp")


class MetaData(BaseModel):
    """Metadata for paginated responses."""
    page: int = Field(..., description="Current page number")
    page_size: int = Field(..., description="Number of items per page")
    total_items: int = Field(..., description="Total number of items")
    total_pages: int = Field(..., description="Total number of pages")

class BaseResponse(BaseModel, Generic[T]):
    """Base response model for all API responses."""
    success: bool = Field(True, description="Whether the request was successful")
    timestamp: datetime = Field(default_factory=datetime.now, description="Response timestamp")


class DataResponse(BaseResponse, Generic[T]):
    """Response model with data field."""
    data: T = Field(..., description="Response data")


class PaginatedResponse(DataResponse, Generic[T]):
    """Response model for paginated data."""
    meta: MetaData = Field(..., description="Pagination metadata")


class MessageResponse(BaseResponse):
    """Response model with only a message."""
    message: str = Field(..., description="Response message")


class HealthCheckResponse(BaseResponse):
    """Health check response model."""
    status: str = Field("ok", description="Service status")
    version: str = Field(..., description="API version")
    uptime: float = Field(..., description="Service uptime in seconds")
    timestamp: datetime = Field(default_factory=datetime.now, description="Current server time")
    components: Dict[str, Dict[str, Any]] = Field(
        ..., description="Status of various system components"
    )