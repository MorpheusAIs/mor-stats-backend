"""
Error handling middleware for the FastAPI application.
"""
import logging
import traceback
from typing import Callable

from fastapi import FastAPI, Request, Response
from fastapi.responses import JSONResponse
from starlette.middleware.base import BaseHTTPMiddleware

from app.core.exceptions import BaseAppException

logger = logging.getLogger(__name__)


class ErrorHandlerMiddleware(BaseHTTPMiddleware):
    """
    Middleware for handling exceptions and returning standardized error responses.
    """
    
    def __init__(self, app: FastAPI):
        super().__init__(app)
    
    async def dispatch(self, request: Request, call_next: Callable) -> Response:
        """
        Process the request and handle any exceptions.
        
        Args:
            request: The incoming request
            call_next: The next middleware or route handler
            
        Returns:
            Response: The API response
        """
        try:
            return await call_next(request)
        
        except BaseAppException as exc:
            # Handle our custom exceptions
            logger.error(
                f"Application error: {exc.message}",
                extra={
                    "status_code": exc.status_code,
                    "details": exc.details,
                    "path": request.url.path
                }
            )
            return JSONResponse(
                status_code=exc.status_code,
                content=exc.to_dict()
            )
        
        except Exception as exc:
            # Handle unexpected exceptions
            logger.error(
                f"Unhandled exception: {str(exc)}",
                extra={
                    "path": request.url.path,
                    "traceback": traceback.format_exc()
                }
            )
            
            # Return a generic error response
            return JSONResponse(
                status_code=500,
                content={
                    "error": "Internal server error",
                    "status_code": 500,
                    "details": {"message": str(exc)}
                }
            )


def add_error_handler(app: FastAPI) -> None:
    """
    Add the error handler middleware to the FastAPI application.
    
    Args:
        app: The FastAPI application
    """
    app.add_middleware(ErrorHandlerMiddleware)