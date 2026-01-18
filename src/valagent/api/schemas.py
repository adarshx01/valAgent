"""
API request and response schemas using Pydantic.
"""

from datetime import datetime
from typing import Any

from pydantic import BaseModel, Field

from valagent.database.models import ValidationStatus


# ============================================================================
# Health & Status
# ============================================================================

class HealthResponse(BaseModel):
    """Health check response."""

    status: str
    version: str
    timestamp: datetime


class ConnectionStatus(BaseModel):
    """Database connection status."""

    connected: bool
    message: str


class ConnectionsResponse(BaseModel):
    """Response for connection test."""

    source: ConnectionStatus
    target: ConnectionStatus


# ============================================================================
# Schema
# ============================================================================

class SchemaResponse(BaseModel):
    """Database schema response."""

    database: str
    tables: dict[str, Any]
    views: list[str]
    schemas: list[str]


class SchemaComparisonResponse(BaseModel):
    """Schema comparison response."""

    schema_comparison: dict[str, Any]
    transformation_patterns: list[dict[str, Any]]
    recommendations: list[str]
    risks: list[str]


# ============================================================================
# Validation
# ============================================================================

class BusinessRulesRequest(BaseModel):
    """Request to analyze business rules."""

    rules: list[str] = Field(
        ...,
        description="List of business rules in natural language",
        min_length=1,
        examples=[
            ["All customers from source should exist in target"],
            ["Total sales amount should match between source and target"],
        ],
    )


class CreateValidationRequest(BaseModel):
    """Request to create a new validation run."""

    name: str = Field(
        ...,
        description="Name of the validation run",
        min_length=1,
        max_length=200,
    )
    description: str | None = Field(
        default=None,
        description="Optional description",
        max_length=1000,
    )
    business_rules: list[str] = Field(
        ...,
        description="List of business rules to validate",
        min_length=1,
    )


class ExecuteValidationRequest(BaseModel):
    """Request to execute a validation run."""

    parallel_tests: int = Field(
        default=5,
        ge=1,
        le=20,
        description="Number of tests to run in parallel",
    )


class QuickValidationRequest(BaseModel):
    """Request for quick validation (create + execute in one call)."""

    name: str = Field(
        default="Quick Validation",
        description="Name of the validation run",
    )
    business_rules: list[str] = Field(
        ...,
        description="List of business rules to validate",
        min_length=1,
    )


class TestCaseResponse(BaseModel):
    """Response for a single test case."""

    id: str
    name: str
    description: str
    business_rule: str
    source_query: str | None
    target_query: str | None
    validation_type: str
    status: ValidationStatus
    actual_result: Any | None
    error_message: str | None
    execution_time_ms: float
    executed_at: datetime | None
    evidence: dict[str, Any]


class ValidationRunResponse(BaseModel):
    """Response for a validation run."""

    id: str
    name: str
    description: str | None
    business_rules: list[str]
    status: ValidationStatus
    total_tests: int
    passed_tests: int
    failed_tests: int
    error_tests: int
    skipped_tests: int
    started_at: datetime | None
    completed_at: datetime | None
    execution_time_ms: float
    test_cases: list[TestCaseResponse] = Field(default_factory=list)


class ValidationRunListResponse(BaseModel):
    """Response for listing validation runs."""

    runs: list[ValidationRunResponse]
    total: int
    limit: int
    offset: int


class ValidationStatisticsResponse(BaseModel):
    """Response for validation statistics."""

    total_runs: int
    runs_by_status: dict[str, int]
    total_tests: int
    passed_tests: int
    failed_tests: int
    error_tests: int
    runs_last_7_days: int
    pass_rate: float


# ============================================================================
# Query
# ============================================================================

class NaturalLanguageQueryRequest(BaseModel):
    """Request for natural language query."""

    query: str = Field(
        ...,
        description="Natural language query",
        min_length=5,
        max_length=2000,
    )


class QueryExecutionRequest(BaseModel):
    """Request to execute a specific SQL query."""

    query: str = Field(
        ...,
        description="SQL query to execute",
        min_length=5,
    )
    database: str = Field(
        ...,
        description="Database to execute on (source or target)",
        pattern="^(source|target)$",
    )
    limit: int = Field(
        default=100,
        ge=1,
        le=10000,
        description="Maximum number of rows to return",
    )


class QueryResultResponse(BaseModel):
    """Response for query execution."""

    query: str
    database: str
    success: bool
    rows: list[dict[str, Any]]
    row_count: int
    columns: list[str]
    execution_time_ms: float
    error: str | None
    timestamp: datetime


class NaturalLanguageQueryResponse(BaseModel):
    """Response for natural language query."""

    understood_intent: str
    queries: list[dict[str, Any]]


# ============================================================================
# Test Generation
# ============================================================================

class GenerateTestsRequest(BaseModel):
    """Request to generate comprehensive tests."""

    focus_areas: list[str] | None = Field(
        default=None,
        description="Specific areas to focus tests on",
    )


class GenerateTestsResponse(BaseModel):
    """Response for test generation."""

    validation_categories: list[dict[str, Any]]
    total_tests: int
    estimated_coverage: str


class AnalysisResponse(BaseModel):
    """Response for business rule analysis."""

    parsed_rules: list[dict[str, Any]]
    test_cases: list[dict[str, Any]]
    summary: dict[str, Any]


# ============================================================================
# Error Responses
# ============================================================================

class ErrorResponse(BaseModel):
    """Standard error response."""

    error: str
    detail: str | None = None
    timestamp: datetime = Field(default_factory=datetime.utcnow)


class ValidationErrorResponse(BaseModel):
    """Validation error response."""

    error: str = "Validation Error"
    detail: list[dict[str, Any]]
    timestamp: datetime = Field(default_factory=datetime.utcnow)
