"""
Custom exceptions for ETL Validator.

Provides a hierarchy of exceptions for different error scenarios.
"""

from typing import Any


class ETLValidatorError(Exception):
    """Base exception for ETL Validator."""

    def __init__(
        self,
        message: str,
        details: dict[str, Any] | None = None,
        error_code: str | None = None,
    ):
        self.message = message
        self.details = details or {}
        self.error_code = error_code or "ETL_ERROR"
        super().__init__(self.message)

    def to_dict(self) -> dict[str, Any]:
        """Convert exception to dictionary."""
        return {
            "error_code": self.error_code,
            "message": self.message,
            "details": self.details,
        }


class DatabaseConnectionError(ETLValidatorError):
    """Exception for database connection failures."""

    def __init__(self, message: str, database: str, details: dict[str, Any] | None = None):
        super().__init__(
            message=message,
            details={"database": database, **(details or {})},
            error_code="DB_CONNECTION_ERROR",
        )


class SchemaExtractionError(ETLValidatorError):
    """Exception for schema extraction failures."""

    def __init__(self, message: str, database: str, details: dict[str, Any] | None = None):
        super().__init__(
            message=message,
            details={"database": database, **(details or {})},
            error_code="SCHEMA_EXTRACTION_ERROR",
        )


class QueryGenerationError(ETLValidatorError):
    """Exception for SQL query generation failures."""

    def __init__(self, message: str, rule: str | None = None, details: dict[str, Any] | None = None):
        super().__init__(
            message=message,
            details={"rule": rule, **(details or {})},
            error_code="QUERY_GENERATION_ERROR",
        )


class QueryExecutionError(ETLValidatorError):
    """Exception for query execution failures."""

    def __init__(
        self,
        message: str,
        query: str | None = None,
        database: str | None = None,
        details: dict[str, Any] | None = None,
    ):
        super().__init__(
            message=message,
            details={"query": query, "database": database, **(details or {})},
            error_code="QUERY_EXECUTION_ERROR",
        )


class ValidationError(ETLValidatorError):
    """Exception for validation failures."""

    def __init__(self, message: str, test_case: str | None = None, details: dict[str, Any] | None = None):
        super().__init__(
            message=message,
            details={"test_case": test_case, **(details or {})},
            error_code="VALIDATION_ERROR",
        )


class LLMError(ETLValidatorError):
    """Exception for LLM-related failures."""

    def __init__(self, message: str, provider: str | None = None, details: dict[str, Any] | None = None):
        super().__init__(
            message=message,
            details={"provider": provider, **(details or {})},
            error_code="LLM_ERROR",
        )


class RateLimitError(ETLValidatorError):
    """Exception for rate limit exceeded."""

    def __init__(self, message: str = "Rate limit exceeded", retry_after: int | None = None):
        super().__init__(
            message=message,
            details={"retry_after": retry_after},
            error_code="RATE_LIMIT_ERROR",
        )
