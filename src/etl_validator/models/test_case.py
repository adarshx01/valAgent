"""
Test case models.

These models represent generated test cases and validation queries.
"""

from enum import Enum
from typing import Any
from pydantic import BaseModel, Field


class TestCaseType(str, Enum):
    """Types of test cases."""

    ROW_COUNT = "row_count"
    DATA_MATCH = "data_match"
    AGGREGATION = "aggregation"
    NULL_CHECK = "null_check"
    UNIQUE_CHECK = "unique_check"
    REFERENTIAL_INTEGRITY = "referential_integrity"
    TRANSFORMATION = "transformation"
    FORMAT_VALIDATION = "format_validation"
    RANGE_CHECK = "range_check"
    DUPLICATE_CHECK = "duplicate_check"
    COMPLETENESS = "completeness"
    CONSISTENCY = "consistency"
    CUSTOM = "custom"


class TestCaseStatus(str, Enum):
    """Status of test case execution."""

    PENDING = "pending"
    RUNNING = "running"
    PASSED = "passed"
    FAILED = "failed"
    ERROR = "error"
    SKIPPED = "skipped"


class ValidationQuery(BaseModel):
    """Represents a SQL query for validation."""

    id: str = Field(..., description="Query identifier")
    database: str = Field(..., description="Target database (source/target)")
    sql: str = Field(..., description="SQL query")
    purpose: str = Field(..., description="Purpose of this query")
    expected_columns: list[str] = Field(
        default_factory=list, description="Expected result columns"
    )
    timeout: int | None = Field(None, description="Query timeout in seconds")
    parameters: dict[str, Any] = Field(
        default_factory=dict, description="Query parameters"
    )


class QueryPair(BaseModel):
    """Represents a pair of queries for source and target comparison."""

    id: str = Field(..., description="Query pair identifier")
    source_query: ValidationQuery = Field(..., description="Query for source database")
    target_query: ValidationQuery = Field(..., description="Query for target database")
    comparison_type: str = Field(
        default="exact", description="How to compare results (exact, subset, aggregate)"
    )
    comparison_columns: list[str] = Field(
        default_factory=list, description="Columns to compare"
    )
    key_columns: list[str] = Field(
        default_factory=list, description="Key columns for row matching"
    )
    tolerance: float | None = Field(
        None, description="Tolerance for numeric comparisons"
    )


class TestCase(BaseModel):
    """Represents a complete test case for validation."""

    id: str = Field(..., description="Test case identifier")
    name: str = Field(..., description="Test case name")
    description: str = Field(..., description="Test case description")
    rule_id: str = Field(..., description="Associated business rule ID")
    test_type: TestCaseType = Field(..., description="Type of test")
    status: TestCaseStatus = Field(
        default=TestCaseStatus.PENDING, description="Execution status"
    )

    # Queries
    query_pairs: list[QueryPair] = Field(
        default_factory=list, description="Query pairs for comparison"
    )
    standalone_queries: list[ValidationQuery] = Field(
        default_factory=list, description="Standalone queries (non-comparative)"
    )

    # Validation criteria
    expected_result: Any = Field(None, description="Expected result value")
    validation_logic: str | None = Field(
        None, description="Logic for validating results"
    )
    pass_criteria: str | None = Field(
        None, description="Criteria for passing the test"
    )
    fail_message_template: str | None = Field(
        None, description="Template for failure message"
    )

    # Metadata
    priority: int = Field(default=1, description="Execution priority (lower = higher priority)")
    tags: list[str] = Field(default_factory=list, description="Test tags")
    metadata: dict[str, Any] = Field(
        default_factory=dict, description="Additional metadata"
    )

    def get_all_queries(self) -> list[ValidationQuery]:
        """Get all queries from this test case."""
        queries = []
        for pair in self.query_pairs:
            queries.append(pair.source_query)
            queries.append(pair.target_query)
        queries.extend(self.standalone_queries)
        return queries

    def to_execution_summary(self) -> dict[str, Any]:
        """Generate summary for execution planning."""
        return {
            "id": self.id,
            "name": self.name,
            "type": self.test_type.value,
            "status": self.status.value,
            "query_count": len(self.get_all_queries()),
            "priority": self.priority,
        }


class TestSuite(BaseModel):
    """Collection of test cases for a validation run."""

    id: str = Field(..., description="Test suite identifier")
    name: str = Field(..., description="Test suite name")
    description: str | None = Field(None, description="Test suite description")
    rule_set_id: str = Field(..., description="Associated business rule set ID")
    test_cases: list[TestCase] = Field(default_factory=list, description="Test cases")
    created_at: str = Field(..., description="Creation timestamp")
    metadata: dict[str, Any] = Field(
        default_factory=dict, description="Additional metadata"
    )

    def get_test_cases_by_type(self, test_type: TestCaseType) -> list[TestCase]:
        """Get test cases by type."""
        return [tc for tc in self.test_cases if tc.test_type == test_type]

    def get_pending_test_cases(self) -> list[TestCase]:
        """Get all pending test cases."""
        return [tc for tc in self.test_cases if tc.status == TestCaseStatus.PENDING]

    def get_ordered_test_cases(self) -> list[TestCase]:
        """Get test cases ordered by priority."""
        return sorted(self.test_cases, key=lambda tc: tc.priority)

    def to_summary(self) -> dict[str, Any]:
        """Generate summary statistics."""
        status_counts = {}
        type_counts = {}

        for tc in self.test_cases:
            status_counts[tc.status.value] = status_counts.get(tc.status.value, 0) + 1
            type_counts[tc.test_type.value] = type_counts.get(tc.test_type.value, 0) + 1

        return {
            "total_test_cases": len(self.test_cases),
            "by_status": status_counts,
            "by_type": type_counts,
        }
