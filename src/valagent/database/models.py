"""
Data models for database schema and query results.
"""

from datetime import datetime
from enum import Enum
from typing import Any

from pydantic import BaseModel, Field


class ColumnInfo(BaseModel):
    """Information about a database column."""

    name: str
    type: str
    nullable: bool = True
    default: str | None = None
    primary_key: bool = False
    description: str | None = None


class ForeignKeyInfo(BaseModel):
    """Information about a foreign key relationship."""

    columns: list[str]
    referred_table: str
    referred_columns: list[str]


class IndexInfo(BaseModel):
    """Information about a database index."""

    name: str
    columns: list[str]
    unique: bool = False


class TableSchema(BaseModel):
    """Schema information for a database table."""

    name: str
    columns: list[ColumnInfo]
    primary_keys: list[str] = Field(default_factory=list)
    foreign_keys: list[ForeignKeyInfo] = Field(default_factory=list)
    indexes: list[IndexInfo] = Field(default_factory=list)
    row_count: int | None = None
    description: str | None = None


class DatabaseInfo(BaseModel):
    """Comprehensive database information."""

    name: str
    database_type: str = "postgresql"
    tables: dict[str, TableSchema] = Field(default_factory=dict)
    views: list[str] = Field(default_factory=list)
    schemas: list[str] = Field(default_factory=list)
    connection_status: bool = True
    last_sync: datetime | None = None


class QueryResult(BaseModel):
    """Result of a database query execution."""

    query: str
    database: str
    success: bool
    rows: list[dict[str, Any]] = Field(default_factory=list)
    row_count: int = 0
    columns: list[str] = Field(default_factory=list)
    execution_time_ms: float = 0.0
    error: str | None = None
    timestamp: datetime = Field(default_factory=datetime.utcnow)


class ComparisonResult(BaseModel):
    """Result of comparing data between source and target."""

    source_query: str
    target_query: str
    source_row_count: int
    target_row_count: int
    matches: bool
    differences: list[dict[str, Any]] = Field(default_factory=list)
    difference_count: int = 0
    comparison_type: str  # "count", "data", "schema", "aggregation"
    execution_time_ms: float = 0.0


class ValidationStatus(str, Enum):
    """Status of a validation test."""

    PENDING = "pending"
    RUNNING = "running"
    PASSED = "passed"
    FAILED = "failed"
    ERROR = "error"
    SKIPPED = "skipped"


class TestCase(BaseModel):
    """A single test case for validation."""

    id: str
    name: str
    description: str
    business_rule: str
    source_query: str | None = None
    target_query: str | None = None
    validation_type: str
    expected_result: Any = None
    status: ValidationStatus = ValidationStatus.PENDING
    actual_result: Any = None
    error_message: str | None = None
    execution_time_ms: float = 0.0
    executed_at: datetime | None = None
    evidence: dict[str, Any] = Field(default_factory=dict)


class ValidationRun(BaseModel):
    """A complete validation run with multiple test cases."""

    id: str
    name: str
    description: str | None = None
    business_rules: list[str]
    test_cases: list[TestCase] = Field(default_factory=list)
    status: ValidationStatus = ValidationStatus.PENDING
    total_tests: int = 0
    passed_tests: int = 0
    failed_tests: int = 0
    error_tests: int = 0
    skipped_tests: int = 0
    started_at: datetime | None = None
    completed_at: datetime | None = None
    execution_time_ms: float = 0.0
    created_by: str | None = None
