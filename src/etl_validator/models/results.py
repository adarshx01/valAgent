"""
Test results and reporting models.

These models capture test execution results and generate detailed reports.
"""

from enum import Enum
from typing import Any
from datetime import datetime
from pydantic import BaseModel, Field


class ResultStatus(str, Enum):
    """Overall result status."""

    PASSED = "passed"
    FAILED = "failed"
    PARTIAL = "partial"
    ERROR = "error"
    SKIPPED = "skipped"


class ExecutionProof(BaseModel):
    """Proof of query execution with results."""

    query_id: str = Field(..., description="Query identifier")
    database: str = Field(..., description="Database where query was executed")
    sql: str = Field(..., description="Executed SQL query")
    execution_time_ms: float = Field(..., description="Execution time in milliseconds")
    row_count: int = Field(..., description="Number of rows returned")
    sample_data: list[dict[str, Any]] = Field(
        default_factory=list, description="Sample rows from result"
    )
    column_names: list[str] = Field(
        default_factory=list, description="Result column names"
    )
    executed_at: str = Field(..., description="Execution timestamp")
    success: bool = Field(True, description="Whether execution succeeded")
    error_message: str | None = Field(None, description="Error message if failed")

    def to_display_string(self, max_rows: int = 5) -> str:
        """Generate display string for reporting."""
        lines = [
            f"Query ID: {self.query_id}",
            f"Database: {self.database}",
            f"SQL: {self.sql[:200]}{'...' if len(self.sql) > 200 else ''}",
            f"Executed at: {self.executed_at}",
            f"Execution time: {self.execution_time_ms:.2f}ms",
            f"Rows returned: {self.row_count}",
        ]

        if not self.success:
            lines.append(f"Error: {self.error_message}")
        elif self.sample_data:
            lines.append("Sample data:")
            for i, row in enumerate(self.sample_data[:max_rows]):
                lines.append(f"  Row {i + 1}: {row}")
            if len(self.sample_data) > max_rows:
                lines.append(f"  ... and {len(self.sample_data) - max_rows} more rows")

        return "\n".join(lines)


class ComparisonDetail(BaseModel):
    """Details of comparison between source and target."""

    comparison_type: str = Field(..., description="Type of comparison performed")
    source_value: Any = Field(None, description="Value from source")
    target_value: Any = Field(None, description="Value from target")
    matched: bool = Field(..., description="Whether values matched")
    difference: str | None = Field(None, description="Description of difference")
    column_name: str | None = Field(None, description="Column being compared")
    row_key: str | None = Field(None, description="Row identifier for context")


class TestResult(BaseModel):
    """Result of a single test case execution."""

    test_case_id: str = Field(..., description="Test case identifier")
    test_case_name: str = Field(..., description="Test case name")
    rule_id: str = Field(..., description="Associated business rule ID")
    status: ResultStatus = Field(..., description="Test result status")

    # Execution details
    started_at: str = Field(..., description="Start timestamp")
    completed_at: str = Field(..., description="Completion timestamp")
    duration_ms: float = Field(..., description="Total duration in milliseconds")

    # Proof of execution
    execution_proofs: list[ExecutionProof] = Field(
        default_factory=list, description="Query execution proofs"
    )

    # Comparison results
    comparisons: list[ComparisonDetail] = Field(
        default_factory=list, description="Comparison details"
    )

    # Summary
    message: str = Field(..., description="Result summary message")
    details: str | None = Field(None, description="Detailed explanation")
    recommendations: list[str] = Field(
        default_factory=list, description="Recommendations for failures"
    )

    # Metrics
    source_row_count: int | None = Field(None, description="Rows from source")
    target_row_count: int | None = Field(None, description="Rows from target")
    matched_rows: int | None = Field(None, description="Number of matched rows")
    mismatched_rows: int | None = Field(None, description="Number of mismatched rows")

    # Metadata
    metadata: dict[str, Any] = Field(
        default_factory=dict, description="Additional metadata"
    )

    def is_passed(self) -> bool:
        """Check if test passed."""
        return self.status == ResultStatus.PASSED

    def to_summary(self) -> dict[str, Any]:
        """Generate summary for reporting."""
        return {
            "test_case_id": self.test_case_id,
            "test_case_name": self.test_case_name,
            "status": self.status.value,
            "duration_ms": self.duration_ms,
            "message": self.message,
            "source_rows": self.source_row_count,
            "target_rows": self.target_row_count,
        }


class TestExecutionSummary(BaseModel):
    """Summary of test execution for a rule set."""

    total_tests: int = Field(..., description="Total number of tests")
    passed: int = Field(0, description="Number of passed tests")
    failed: int = Field(0, description="Number of failed tests")
    errors: int = Field(0, description="Number of tests with errors")
    skipped: int = Field(0, description="Number of skipped tests")
    
    total_duration_ms: float = Field(..., description="Total execution duration")
    average_duration_ms: float = Field(..., description="Average test duration")

    pass_rate: float = Field(..., description="Pass rate percentage")
    
    critical_failures: int = Field(0, description="Number of critical test failures")
    
    def to_display(self) -> str:
        """Generate display string."""
        return (
            f"Test Execution Summary\n"
            f"{'=' * 40}\n"
            f"Total Tests: {self.total_tests}\n"
            f"Passed: {self.passed} ({self.pass_rate:.1f}%)\n"
            f"Failed: {self.failed}\n"
            f"Errors: {self.errors}\n"
            f"Skipped: {self.skipped}\n"
            f"Critical Failures: {self.critical_failures}\n"
            f"Total Duration: {self.total_duration_ms:.2f}ms\n"
            f"Average Duration: {self.average_duration_ms:.2f}ms\n"
        )


class ScenarioCoverage(BaseModel):
    """Coverage information for validation scenarios."""

    scenario_name: str = Field(..., description="Scenario name")
    description: str = Field(..., description="Scenario description")
    covered: bool = Field(..., description="Whether scenario was covered")
    test_case_ids: list[str] = Field(
        default_factory=list, description="Test cases covering this scenario"
    )
    coverage_percentage: float = Field(0.0, description="Coverage percentage")


class ValidationReport(BaseModel):
    """Complete validation report with all details."""

    # Report metadata
    report_id: str = Field(..., description="Report identifier")
    report_name: str = Field(..., description="Report name")
    generated_at: str = Field(..., description="Report generation timestamp")
    
    # Context
    source_database: str = Field(..., description="Source database identifier")
    target_database: str = Field(..., description="Target database identifier")
    rule_set_id: str = Field(..., description="Business rule set ID")
    rule_set_name: str = Field(..., description="Business rule set name")

    # Overall status
    overall_status: ResultStatus = Field(..., description="Overall validation status")
    
    # Summary
    execution_summary: TestExecutionSummary = Field(
        ..., description="Execution summary"
    )
    
    # Detailed results
    test_results: list[TestResult] = Field(
        default_factory=list, description="Individual test results"
    )
    
    # Scenario coverage
    scenarios_covered: list[ScenarioCoverage] = Field(
        default_factory=list, description="Scenario coverage information"
    )
    
    # AI Analysis
    ai_analysis: str | None = Field(None, description="AI-generated analysis")
    ai_recommendations: list[str] = Field(
        default_factory=list, description="AI-generated recommendations"
    )
    
    # Metadata
    execution_metadata: dict[str, Any] = Field(
        default_factory=dict, description="Execution metadata"
    )

    def get_failed_tests(self) -> list[TestResult]:
        """Get all failed test results."""
        return [r for r in self.test_results if r.status == ResultStatus.FAILED]

    def get_passed_tests(self) -> list[TestResult]:
        """Get all passed test results."""
        return [r for r in self.test_results if r.status == ResultStatus.PASSED]

    def get_error_tests(self) -> list[TestResult]:
        """Get all tests with errors."""
        return [r for r in self.test_results if r.status == ResultStatus.ERROR]

    def to_markdown(self) -> str:
        """Generate Markdown report."""
        lines = [
            f"# ETL Validation Report",
            f"",
            f"**Report ID:** {self.report_id}",
            f"**Generated:** {self.generated_at}",
            f"",
            f"## Overview",
            f"",
            f"- **Source Database:** {self.source_database}",
            f"- **Target Database:** {self.target_database}",
            f"- **Rule Set:** {self.rule_set_name}",
            f"- **Overall Status:** {self.overall_status.value.upper()}",
            f"",
            f"## Execution Summary",
            f"",
            f"| Metric | Value |",
            f"|--------|-------|",
            f"| Total Tests | {self.execution_summary.total_tests} |",
            f"| Passed | {self.execution_summary.passed} |",
            f"| Failed | {self.execution_summary.failed} |",
            f"| Errors | {self.execution_summary.errors} |",
            f"| Skipped | {self.execution_summary.skipped} |",
            f"| Pass Rate | {self.execution_summary.pass_rate:.1f}% |",
            f"| Total Duration | {self.execution_summary.total_duration_ms:.2f}ms |",
            f"",
        ]

        # Scenarios covered
        if self.scenarios_covered:
            lines.extend([
                f"## Scenarios Covered",
                f"",
            ])
            for scenario in self.scenarios_covered:
                status = "✅" if scenario.covered else "❌"
                lines.append(f"- {status} **{scenario.scenario_name}**: {scenario.description}")
            lines.append("")

        # Failed tests
        failed_tests = self.get_failed_tests()
        if failed_tests:
            lines.extend([
                f"## Failed Tests",
                f"",
            ])
            for test in failed_tests:
                lines.extend([
                    f"### {test.test_case_name}",
                    f"",
                    f"**Status:** ❌ FAILED",
                    f"**Message:** {test.message}",
                    f"",
                ])
                if test.details:
                    lines.append(f"**Details:** {test.details}")
                    lines.append("")
                if test.recommendations:
                    lines.append("**Recommendations:**")
                    for rec in test.recommendations:
                        lines.append(f"- {rec}")
                    lines.append("")

        # Passed tests
        passed_tests = self.get_passed_tests()
        if passed_tests:
            lines.extend([
                f"## Passed Tests",
                f"",
            ])
            for test in passed_tests:
                lines.append(f"- ✅ **{test.test_case_name}**: {test.message}")
            lines.append("")

        # AI Analysis
        if self.ai_analysis:
            lines.extend([
                f"## AI Analysis",
                f"",
                self.ai_analysis,
                f"",
            ])

        if self.ai_recommendations:
            lines.extend([
                f"## AI Recommendations",
                f"",
            ])
            for rec in self.ai_recommendations:
                lines.append(f"- {rec}")
            lines.append("")

        return "\n".join(lines)

    def to_json_summary(self) -> dict[str, Any]:
        """Generate JSON summary for API response."""
        return {
            "report_id": self.report_id,
            "report_name": self.report_name,
            "generated_at": self.generated_at,
            "overall_status": self.overall_status.value,
            "summary": {
                "total_tests": self.execution_summary.total_tests,
                "passed": self.execution_summary.passed,
                "failed": self.execution_summary.failed,
                "errors": self.execution_summary.errors,
                "skipped": self.execution_summary.skipped,
                "pass_rate": self.execution_summary.pass_rate,
                "duration_ms": self.execution_summary.total_duration_ms,
            },
            "scenarios_covered": len([s for s in self.scenarios_covered if s.covered]),
            "total_scenarios": len(self.scenarios_covered),
            "has_critical_failures": self.execution_summary.critical_failures > 0,
        }
