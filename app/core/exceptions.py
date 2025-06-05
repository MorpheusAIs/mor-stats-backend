"""
Custom exceptions for the MOR Stats Backend.
"""
from typing import Any, Dict, List, Optional, Union

from fastapi import HTTPException, status


class BaseAppException(Exception):
    """Base exception for all application exceptions."""
    
    def __init__(
        self,
        message: str = "An unexpected error occurred",
        status_code: int = status.HTTP_500_INTERNAL_SERVER_ERROR,
        details: Optional[Dict[str, Any]] = None
    ):
        self.message = message
        self.status_code = status_code
        self.details = details or {}
        super().__init__(self.message)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert exception to dictionary for API response."""
        return {
            "error": self.message,
            "status_code": self.status_code,
            "details": self.details
        }
    
    def to_http_exception(self) -> HTTPException:
        """Convert to FastAPI HTTPException."""
        return HTTPException(
            status_code=self.status_code,
            detail={"error": self.message, "details": self.details}
        )


class DatabaseError(BaseAppException):
    """Exception raised for database-related errors."""
    
    def __init__(
        self,
        message: str = "Database error",
        details: Optional[Dict[str, Any]] = None
    ):
        super().__init__(
            message=message,
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            details=details
        )


class Web3Error(BaseAppException):
    """Exception raised for Web3/blockchain-related errors."""
    
    def __init__(
        self,
        message: str = "Blockchain interaction error",
        details: Optional[Dict[str, Any]] = None
    ):
        super().__init__(
            message=message,
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            details=details
        )


class CacheError(BaseAppException):
    """Exception raised for cache-related errors."""
    
    def __init__(
        self,
        message: str = "Cache error",
        details: Optional[Dict[str, Any]] = None
    ):
        super().__init__(
            message=message,
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            details=details
        )


class ResourceNotFoundError(BaseAppException):
    """Exception raised when a requested resource is not found."""
    
    def __init__(
        self,
        message: str = "Resource not found",
        resource_type: str = "resource",
        resource_id: Optional[Union[str, int]] = None
    ):
        details = {"resource_type": resource_type}
        if resource_id is not None:
            details["resource_id"] = resource_id
        
        super().__init__(
            message=message,
            status_code=status.HTTP_404_NOT_FOUND,
            details=details
        )


class ValidationError(BaseAppException):
    """Exception raised for validation errors."""
    
    def __init__(
        self,
        message: str = "Validation error",
        errors: Optional[List[Dict[str, Any]]] = None
    ):
        super().__init__(
            message=message,
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            details={"errors": errors or []}
        )


class ExternalAPIError(BaseAppException):
    """Exception raised for errors from external APIs."""
    
    def __init__(
        self,
        message: str = "External API error",
        api_name: str = "unknown",
        details: Optional[Dict[str, Any]] = None
    ):
        error_details = details or {}
        error_details["api_name"] = api_name
        
        super().__init__(
            message=message,
            status_code=status.HTTP_502_BAD_GATEWAY,
            details=error_details
        )


class ConfigurationError(BaseAppException):
    """Exception raised for configuration errors."""
    
    def __init__(
        self,
        message: str = "Configuration error",
        details: Optional[Dict[str, Any]] = None
    ):
        super().__init__(
            message=message,
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            details=details
        )