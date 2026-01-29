"""Data models for ETL Validator."""

from .schema import (
    ColumnInfo,
    TableInfo,
    DatabaseSchema,
    SchemaComparisonResult,
)
from .rules import (
    BusinessRule,
    BusinessRuleSet,
    RuleCategory,
    RulePriority,
)
from .test_case import (
    TestCase,
    TestCaseStatus,
    TestCaseType,
    QueryPair,
    ValidationQuery,
)
from .results import (
    TestResult,
    TestExecutionSummary,
    ValidationReport,
    ExecutionProof,
)

__all__ = [
    # Schema models
    "ColumnInfo",
    "TableInfo",
    "DatabaseSchema",
    "SchemaComparisonResult",
    # Rule models
    "BusinessRule",
    "BusinessRuleSet",
    "RuleCategory",
    "RulePriority",
    # Test case models
    "TestCase",
    "TestCaseStatus",
    "TestCaseType",
    "QueryPair",
    "ValidationQuery",
    # Result models
    "TestResult",
    "TestExecutionSummary",
    "ValidationReport",
    "ExecutionProof",
]
