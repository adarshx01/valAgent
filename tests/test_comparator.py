"""
Tests for the data comparator.
"""

import pytest
from valagent.engine.comparator import DataComparator
from valagent.database.models import QueryResult
from datetime import datetime


@pytest.fixture
def comparator():
    """Create a DataComparator instance."""
    return DataComparator()


def make_query_result(
    rows: list,
    query: str = "SELECT 1",
    database: str = "source",
    success: bool = True,
) -> QueryResult:
    """Helper to create QueryResult objects."""
    columns = list(rows[0].keys()) if rows else []
    return QueryResult(
        query=query,
        database=database,
        success=success,
        rows=rows,
        row_count=len(rows),
        columns=columns,
        execution_time_ms=10.0,
        timestamp=datetime.utcnow(),
    )


class TestCompareCount:
    """Tests for count comparison."""

    def test_equal_counts(self, comparator):
        """Test matching counts."""
        source = make_query_result([{"count": 100}], database="source")
        target = make_query_result([{"count": 100}], database="target")

        result = comparator.compare_counts(source, target)
        
        assert result.matches is True
        assert result.source_row_count == 100
        assert result.target_row_count == 100
        assert len(result.differences) == 0

    def test_different_counts(self, comparator):
        """Test mismatched counts."""
        source = make_query_result([{"count": 100}], database="source")
        target = make_query_result([{"count": 95}], database="target")

        result = comparator.compare_counts(source, target)
        
        assert result.matches is False
        assert result.source_row_count == 100
        assert result.target_row_count == 95
        assert len(result.differences) == 1
        assert result.differences[0]["type"] == "count_mismatch"

    def test_count_with_tolerance(self, comparator):
        """Test counts within tolerance."""
        source = make_query_result([{"count": 100}], database="source")
        target = make_query_result([{"count": 98}], database="target")

        result = comparator.compare_counts(source, target, tolerance=0.05)
        
        assert result.matches is True  # 2% difference within 5% tolerance


class TestCompareAggregations:
    """Tests for aggregation comparison."""

    def test_matching_aggregations(self, comparator):
        """Test matching aggregation values."""
        source = make_query_result([{"sum": 1000, "avg": 50}], database="source")
        target = make_query_result([{"sum": 1000, "avg": 50}], database="target")

        result = comparator.compare_aggregations(source, target)
        
        assert result.matches is True
        assert len(result.differences) == 0

    def test_different_aggregations(self, comparator):
        """Test mismatched aggregation values."""
        source = make_query_result([{"sum": 1000}], database="source")
        target = make_query_result([{"sum": 900}], database="target")

        result = comparator.compare_aggregations(source, target)
        
        assert result.matches is False
        assert len(result.differences) == 1


class TestCompareDataRows:
    """Tests for row-by-row comparison."""

    def test_matching_rows(self, comparator):
        """Test matching data rows."""
        source = make_query_result([
            {"id": 1, "name": "Alice"},
            {"id": 2, "name": "Bob"},
        ], database="source")
        target = make_query_result([
            {"id": 1, "name": "Alice"},
            {"id": 2, "name": "Bob"},
        ], database="target")

        result = comparator.compare_data_rows(
            source, target,
            key_columns=["id"],
            compare_columns=["name"],
        )
        
        assert result.matches is True
        assert result.difference_count == 0

    def test_missing_row(self, comparator):
        """Test missing row in target."""
        source = make_query_result([
            {"id": 1, "name": "Alice"},
            {"id": 2, "name": "Bob"},
        ], database="source")
        target = make_query_result([
            {"id": 1, "name": "Alice"},
        ], database="target")

        result = comparator.compare_data_rows(
            source, target,
            key_columns=["id"],
        )
        
        assert result.matches is False
        assert any(d["type"] == "missing_in_target" for d in result.differences)

    def test_extra_row(self, comparator):
        """Test extra row in target."""
        source = make_query_result([
            {"id": 1, "name": "Alice"},
        ], database="source")
        target = make_query_result([
            {"id": 1, "name": "Alice"},
            {"id": 2, "name": "Bob"},
        ], database="target")

        result = comparator.compare_data_rows(
            source, target,
            key_columns=["id"],
        )
        
        assert result.matches is False
        assert any(d["type"] == "extra_in_target" for d in result.differences)

    def test_value_mismatch(self, comparator):
        """Test value difference in matching rows."""
        source = make_query_result([
            {"id": 1, "name": "Alice"},
        ], database="source")
        target = make_query_result([
            {"id": 1, "name": "ALICE"},
        ], database="target")

        result = comparator.compare_data_rows(
            source, target,
            key_columns=["id"],
            case_sensitive=True,
        )
        
        assert result.matches is False
        assert any(d["type"] == "value_mismatch" for d in result.differences)


class TestValidateTargetOnly:
    """Tests for target-only validation."""

    def test_expect_empty(self, comparator):
        """Test expecting empty result."""
        target = make_query_result([], database="target")
        
        result = comparator.validate_target_only(target, "empty")
        
        assert result.matches is True

    def test_expect_empty_but_has_data(self, comparator):
        """Test expecting empty but found data."""
        target = make_query_result([{"id": 1}], database="target")
        
        result = comparator.validate_target_only(target, "empty")
        
        assert result.matches is False

    def test_expect_not_empty(self, comparator):
        """Test expecting non-empty result."""
        target = make_query_result([{"id": 1}], database="target")
        
        result = comparator.validate_target_only(target, "not_empty")
        
        assert result.matches is True
