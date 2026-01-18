"""
Data comparator for comparing query results between source and target.
Implements various comparison strategies for different validation types.
"""

import hashlib
import json
import logging
from typing import Any
from collections import defaultdict

from valagent.database.models import ComparisonResult, QueryResult

logger = logging.getLogger(__name__)


class DataComparator:
    """
    Compares data between source and target databases.
    Supports multiple comparison strategies for different validation needs.
    """

    def compare_counts(
        self,
        source_result: QueryResult,
        target_result: QueryResult,
        tolerance: float = 0.0,
    ) -> ComparisonResult:
        """
        Compare row counts between source and target.
        
        Args:
            source_result: Query result from source database
            target_result: Query result from target database
            tolerance: Allowed percentage difference (0.0 to 1.0)
            
        Returns:
            ComparisonResult with comparison details
        """
        source_count = 0
        target_count = 0

        # Extract counts from results
        if source_result.success and source_result.rows:
            first_row = source_result.rows[0]
            source_count = first_row.get("count", first_row.get("total", 0))

        if target_result.success and target_result.rows:
            first_row = target_result.rows[0]
            target_count = first_row.get("count", first_row.get("total", 0))

        # Calculate difference
        difference = abs(source_count - target_count)
        if source_count > 0:
            percentage_diff = difference / source_count
        else:
            percentage_diff = 1.0 if target_count > 0 else 0.0

        matches = percentage_diff <= tolerance

        differences = []
        if not matches:
            differences.append({
                "type": "count_mismatch",
                "source_count": source_count,
                "target_count": target_count,
                "difference": difference,
                "percentage_difference": round(percentage_diff * 100, 2),
            })

        return ComparisonResult(
            source_query=source_result.query,
            target_query=target_result.query,
            source_row_count=source_count,
            target_row_count=target_count,
            matches=matches,
            differences=differences,
            difference_count=len(differences),
            comparison_type="count",
            execution_time_ms=source_result.execution_time_ms + target_result.execution_time_ms,
        )

    def compare_aggregations(
        self,
        source_result: QueryResult,
        target_result: QueryResult,
        tolerance: float = 0.001,
    ) -> ComparisonResult:
        """
        Compare aggregation results (sums, averages, etc.) between source and target.
        """
        differences = []

        if not source_result.success or not target_result.success:
            return ComparisonResult(
                source_query=source_result.query,
                target_query=target_result.query,
                source_row_count=source_result.row_count,
                target_row_count=target_result.row_count,
                matches=False,
                differences=[{"type": "execution_error", "error": "Query execution failed"}],
                difference_count=1,
                comparison_type="aggregation",
                execution_time_ms=source_result.execution_time_ms + target_result.execution_time_ms,
            )

        source_row = source_result.rows[0] if source_result.rows else {}
        target_row = target_result.rows[0] if target_result.rows else {}

        # Compare each aggregation column
        for key in set(source_row.keys()) | set(target_row.keys()):
            source_val = source_row.get(key)
            target_val = target_row.get(key)

            if source_val is None and target_val is None:
                continue

            if source_val is None or target_val is None:
                differences.append({
                    "type": "missing_value",
                    "column": key,
                    "source_value": source_val,
                    "target_value": target_val,
                })
                continue

            # Numeric comparison with tolerance
            if isinstance(source_val, (int, float)) and isinstance(target_val, (int, float)):
                if source_val == 0:
                    matches = target_val == 0
                else:
                    diff_ratio = abs(source_val - target_val) / abs(source_val)
                    matches = diff_ratio <= tolerance

                if not matches:
                    differences.append({
                        "type": "value_mismatch",
                        "column": key,
                        "source_value": source_val,
                        "target_value": target_val,
                        "difference": target_val - source_val,
                        "percentage_difference": round(
                            (abs(source_val - target_val) / abs(source_val) * 100)
                            if source_val != 0 else float('inf'),
                            4
                        ),
                    })
            else:
                # String/other comparison
                if str(source_val) != str(target_val):
                    differences.append({
                        "type": "value_mismatch",
                        "column": key,
                        "source_value": source_val,
                        "target_value": target_val,
                    })

        return ComparisonResult(
            source_query=source_result.query,
            target_query=target_result.query,
            source_row_count=1,
            target_row_count=1,
            matches=len(differences) == 0,
            differences=differences,
            difference_count=len(differences),
            comparison_type="aggregation",
            execution_time_ms=source_result.execution_time_ms + target_result.execution_time_ms,
        )

    def compare_data_rows(
        self,
        source_result: QueryResult,
        target_result: QueryResult,
        key_columns: list[str],
        compare_columns: list[str] | None = None,
        case_sensitive: bool = True,
        max_differences: int = 100,
    ) -> ComparisonResult:
        """
        Compare actual data rows between source and target using key columns.
        
        Args:
            source_result: Query result from source
            target_result: Query result from target
            key_columns: Columns to use for matching records
            compare_columns: Columns to compare (all if None)
            case_sensitive: Whether string comparison is case-sensitive
            max_differences: Maximum number of differences to report
        """
        differences = []

        if not source_result.success or not target_result.success:
            return ComparisonResult(
                source_query=source_result.query,
                target_query=target_result.query,
                source_row_count=source_result.row_count,
                target_row_count=target_result.row_count,
                matches=False,
                differences=[{"type": "execution_error"}],
                difference_count=1,
                comparison_type="data",
                execution_time_ms=source_result.execution_time_ms + target_result.execution_time_ms,
            )

        # Build lookup dictionaries
        def make_key(row: dict) -> tuple:
            return tuple(row.get(col) for col in key_columns)

        source_lookup = {make_key(row): row for row in source_result.rows}
        target_lookup = {make_key(row): row for row in target_result.rows}

        source_keys = set(source_lookup.keys())
        target_keys = set(target_lookup.keys())

        # Find missing records
        missing_in_target = source_keys - target_keys
        extra_in_target = target_keys - source_keys
        common_keys = source_keys & target_keys

        for key in list(missing_in_target)[:max_differences]:
            if len(differences) >= max_differences:
                break
            differences.append({
                "type": "missing_in_target",
                "key": dict(zip(key_columns, key)),
                "source_record": source_lookup[key],
            })

        for key in list(extra_in_target)[:max_differences - len(differences)]:
            if len(differences) >= max_differences:
                break
            differences.append({
                "type": "extra_in_target",
                "key": dict(zip(key_columns, key)),
                "target_record": target_lookup[key],
            })

        # Compare common records
        columns_to_compare = compare_columns or [
            col for col in source_result.columns if col not in key_columns
        ]

        for key in common_keys:
            if len(differences) >= max_differences:
                break

            source_row = source_lookup[key]
            target_row = target_lookup[key]

            row_differences = []
            for col in columns_to_compare:
                source_val = source_row.get(col)
                target_val = target_row.get(col)

                if not self._values_equal(source_val, target_val, case_sensitive):
                    row_differences.append({
                        "column": col,
                        "source_value": source_val,
                        "target_value": target_val,
                    })

            if row_differences:
                differences.append({
                    "type": "value_mismatch",
                    "key": dict(zip(key_columns, key)),
                    "differences": row_differences,
                })

        total_differences = (
            len(missing_in_target) +
            len(extra_in_target) +
            sum(1 for d in differences if d["type"] == "value_mismatch")
        )

        return ComparisonResult(
            source_query=source_result.query,
            target_query=target_result.query,
            source_row_count=source_result.row_count,
            target_row_count=target_result.row_count,
            matches=total_differences == 0,
            differences=differences,
            difference_count=total_differences,
            comparison_type="data",
            execution_time_ms=source_result.execution_time_ms + target_result.execution_time_ms,
        )

    def compare_hashes(
        self,
        source_result: QueryResult,
        target_result: QueryResult,
    ) -> ComparisonResult:
        """
        Compare data by computing hash of entire result sets.
        Useful for quick verification of large datasets.
        """
        def compute_hash(rows: list[dict]) -> str:
            # Sort rows by all columns for consistent ordering
            sorted_rows = sorted(rows, key=lambda x: json.dumps(x, sort_keys=True, default=str))
            data_str = json.dumps(sorted_rows, sort_keys=True, default=str)
            return hashlib.sha256(data_str.encode()).hexdigest()

        source_hash = compute_hash(source_result.rows) if source_result.rows else ""
        target_hash = compute_hash(target_result.rows) if target_result.rows else ""

        matches = source_hash == target_hash

        differences = []
        if not matches:
            differences.append({
                "type": "hash_mismatch",
                "source_hash": source_hash,
                "target_hash": target_hash,
                "source_row_count": source_result.row_count,
                "target_row_count": target_result.row_count,
            })

        return ComparisonResult(
            source_query=source_result.query,
            target_query=target_result.query,
            source_row_count=source_result.row_count,
            target_row_count=target_result.row_count,
            matches=matches,
            differences=differences,
            difference_count=len(differences),
            comparison_type="hash",
            execution_time_ms=source_result.execution_time_ms + target_result.execution_time_ms,
        )

    def compare_schema(
        self,
        source_schema: dict[str, Any],
        target_schema: dict[str, Any],
    ) -> ComparisonResult:
        """
        Compare database schemas between source and target.
        """
        differences = []

        source_tables = set(source_schema.get("tables", {}).keys())
        target_tables = set(target_schema.get("tables", {}).keys())

        # Tables only in source
        for table in source_tables - target_tables:
            differences.append({
                "type": "table_missing_in_target",
                "table": table,
            })

        # Tables only in target
        for table in target_tables - source_tables:
            differences.append({
                "type": "table_extra_in_target",
                "table": table,
            })

        # Compare common tables
        for table in source_tables & target_tables:
            source_cols = {
                col["name"]: col
                for col in source_schema["tables"][table].get("columns", [])
            }
            target_cols = {
                col["name"]: col
                for col in target_schema["tables"][table].get("columns", [])
            }

            # Missing columns
            for col_name in set(source_cols.keys()) - set(target_cols.keys()):
                differences.append({
                    "type": "column_missing_in_target",
                    "table": table,
                    "column": col_name,
                })

            # Extra columns
            for col_name in set(target_cols.keys()) - set(source_cols.keys()):
                differences.append({
                    "type": "column_extra_in_target",
                    "table": table,
                    "column": col_name,
                })

            # Type mismatches
            for col_name in set(source_cols.keys()) & set(target_cols.keys()):
                if source_cols[col_name]["type"] != target_cols[col_name]["type"]:
                    differences.append({
                        "type": "column_type_mismatch",
                        "table": table,
                        "column": col_name,
                        "source_type": source_cols[col_name]["type"],
                        "target_type": target_cols[col_name]["type"],
                    })

        return ComparisonResult(
            source_query="schema_introspection",
            target_query="schema_introspection",
            source_row_count=len(source_tables),
            target_row_count=len(target_tables),
            matches=len(differences) == 0,
            differences=differences,
            difference_count=len(differences),
            comparison_type="schema",
            execution_time_ms=0,
        )

    def validate_target_only(
        self,
        target_result: QueryResult,
        expected_condition: str,
        expected_value: Any = None,
    ) -> ComparisonResult:
        """
        Validate target data against an expected condition.
        Used when there's no source to compare against.
        """
        differences = []
        matches = True

        if not target_result.success:
            return ComparisonResult(
                source_query="N/A",
                target_query=target_result.query,
                source_row_count=0,
                target_row_count=0,
                matches=False,
                differences=[{"type": "execution_error", "error": target_result.error}],
                difference_count=1,
                comparison_type="custom",
                execution_time_ms=target_result.execution_time_ms,
            )

        if expected_condition == "empty":
            matches = target_result.row_count == 0
            if not matches:
                differences.append({
                    "type": "expected_empty",
                    "actual_count": target_result.row_count,
                    "sample_rows": target_result.rows[:5],
                })

        elif expected_condition == "not_empty":
            matches = target_result.row_count > 0
            if not matches:
                differences.append({
                    "type": "expected_not_empty",
                    "actual_count": 0,
                })

        elif expected_condition == "count_equals":
            actual = target_result.rows[0].get("count", 0) if target_result.rows else 0
            matches = actual == expected_value
            if not matches:
                differences.append({
                    "type": "count_mismatch",
                    "expected": expected_value,
                    "actual": actual,
                })

        elif expected_condition == "count_greater_than":
            actual = target_result.rows[0].get("count", 0) if target_result.rows else 0
            matches = actual > expected_value
            if not matches:
                differences.append({
                    "type": "count_below_threshold",
                    "threshold": expected_value,
                    "actual": actual,
                })

        elif expected_condition == "all_match":
            # All rows should satisfy some condition - provided as custom logic
            pass

        return ComparisonResult(
            source_query="N/A",
            target_query=target_result.query,
            source_row_count=0,
            target_row_count=target_result.row_count,
            matches=matches,
            differences=differences,
            difference_count=len(differences),
            comparison_type="custom",
            execution_time_ms=target_result.execution_time_ms,
        )

    def _values_equal(
        self,
        val1: Any,
        val2: Any,
        case_sensitive: bool = True,
    ) -> bool:
        """Compare two values for equality."""
        if val1 is None and val2 is None:
            return True
        if val1 is None or val2 is None:
            return False

        # String comparison
        if isinstance(val1, str) and isinstance(val2, str):
            if case_sensitive:
                return val1 == val2
            return val1.lower() == val2.lower()

        # Numeric comparison
        if isinstance(val1, (int, float)) and isinstance(val2, (int, float)):
            # Handle floating point comparison
            if isinstance(val1, float) or isinstance(val2, float):
                return abs(val1 - val2) < 1e-9
            return val1 == val2

        # Default comparison
        return val1 == val2
