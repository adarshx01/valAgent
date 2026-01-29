"""
Query Executor Service for parallel query execution.

Handles efficient execution of validation queries with parallel processing
and proper result collection.
"""

import asyncio
import time
from typing import Any
from datetime import datetime, timezone

from ..core.database import DatabaseManager
from ..core.config import settings
from ..core.exceptions import QueryExecutionError
from ..models.test_case import TestCase, QueryPair, ValidationQuery, TestCaseStatus
from ..models.results import ExecutionProof, ComparisonDetail
from ..utils.logger import get_logger
from ..utils.helpers import generate_uuid, get_timestamp_str, compare_values

logger = get_logger(__name__)


class QueryExecutorService:
    """
    Service for executing validation queries.
    
    Supports parallel execution with configurable concurrency,
    result comparison, and proof of execution generation.
    """

    def __init__(self, db_manager: DatabaseManager):
        self._db_manager = db_manager

    async def execute_query_pair(
        self,
        query_pair: QueryPair,
    ) -> dict[str, Any]:
        """
        Execute a pair of source/target queries and compare results.
        
        Args:
            query_pair: Query pair to execute
            
        Returns:
            Comparison result with proofs
        """
        logger.debug(f"Executing query pair: {query_pair.id}")

        # Execute both queries in parallel
        source_result, target_result = await asyncio.gather(
            self._execute_single_query(query_pair.source_query),
            self._execute_single_query(query_pair.target_query),
            return_exceptions=True,
        )

        # Handle exceptions
        if isinstance(source_result, Exception):
            return {
                "query_pair_id": query_pair.id,
                "success": False,
                "error": f"Source query failed: {str(source_result)}",
                "source_proof": None,
                "target_proof": None,
            }

        if isinstance(target_result, Exception):
            return {
                "query_pair_id": query_pair.id,
                "success": False,
                "error": f"Target query failed: {str(target_result)}",
                "source_proof": source_result.get("proof"),
                "target_proof": None,
            }

        # Compare results
        comparison = await self._compare_results(
            source_data=source_result["data"],
            target_data=target_result["data"],
            comparison_type=query_pair.comparison_type,
            comparison_columns=query_pair.comparison_columns,
            key_columns=query_pair.key_columns,
            tolerance=query_pair.tolerance,
        )

        return {
            "query_pair_id": query_pair.id,
            "success": True,
            "matched": comparison["matched"],
            "source_proof": source_result["proof"],
            "target_proof": target_result["proof"],
            "comparison_details": comparison["details"],
            "source_row_count": source_result["row_count"],
            "target_row_count": target_result["row_count"],
        }

    async def _execute_single_query(
        self,
        query: ValidationQuery,
    ) -> dict[str, Any]:
        """Execute a single validation query."""
        start_time = time.time()

        try:
            if query.database == "source":
                result = await self._db_manager.execute_source_query(
                    query.sql,
                    timeout=query.timeout or settings.query_timeout,
                )
            else:
                result = await self._db_manager.execute_target_query(
                    query.sql,
                    timeout=query.timeout or settings.query_timeout,
                )

            execution_time = (time.time() - start_time) * 1000

            # Convert to list of dicts
            data = [dict(r) for r in result]

            # Get column names
            column_names = list(result[0].keys()) if result else []

            # Create execution proof
            proof = ExecutionProof(
                query_id=query.id,
                database=query.database,
                sql=query.sql,
                execution_time_ms=execution_time,
                row_count=len(data),
                sample_data=data[:10],  # First 10 rows as sample
                column_names=column_names,
                executed_at=get_timestamp_str(),
                success=True,
            )

            logger.debug(
                f"Query {query.id} executed: {len(data)} rows in {execution_time:.2f}ms"
            )

            return {
                "success": True,
                "data": data,
                "row_count": len(data),
                "proof": proof,
            }

        except Exception as e:
            execution_time = (time.time() - start_time) * 1000
            logger.error(f"Query {query.id} failed: {e}")

            proof = ExecutionProof(
                query_id=query.id,
                database=query.database,
                sql=query.sql,
                execution_time_ms=execution_time,
                row_count=0,
                executed_at=get_timestamp_str(),
                success=False,
                error_message=str(e),
            )

            raise QueryExecutionError(
                message=f"Query execution failed",
                query=query.sql[:200],
                database=query.database,
                details={"error": str(e)},
            )

    async def _compare_results(
        self,
        source_data: list[dict],
        target_data: list[dict],
        comparison_type: str,
        comparison_columns: list[str],
        key_columns: list[str],
        tolerance: float | None = None,
    ) -> dict[str, Any]:
        """Compare source and target query results."""
        details = []

        if comparison_type == "count":
            # Simple row count comparison
            matched = len(source_data) == len(target_data)
            details.append(
                ComparisonDetail(
                    comparison_type="row_count",
                    source_value=len(source_data),
                    target_value=len(target_data),
                    matched=matched,
                    difference=None if matched else f"Row count mismatch: {len(source_data)} vs {len(target_data)}",
                )
            )
            return {"matched": matched, "details": details}

        elif comparison_type == "aggregate":
            # Compare aggregate values
            if not source_data or not target_data:
                matched = not source_data and not target_data
                details.append(
                    ComparisonDetail(
                        comparison_type="aggregate",
                        source_value=source_data,
                        target_value=target_data,
                        matched=matched,
                        difference="Empty result sets" if matched else "One result set is empty",
                    )
                )
                return {"matched": matched, "details": details}

            # Compare first row (aggregate result)
            source_row = source_data[0]
            target_row = target_data[0]
            all_matched = True

            for col in comparison_columns or source_row.keys():
                if col in source_row and col in target_row:
                    matched = compare_values(
                        source_row[col], target_row[col], tolerance
                    )
                    if not matched:
                        all_matched = False
                    details.append(
                        ComparisonDetail(
                            comparison_type="aggregate_value",
                            source_value=source_row[col],
                            target_value=target_row[col],
                            matched=matched,
                            column_name=col,
                            difference=None if matched else f"Value mismatch for {col}",
                        )
                    )

            return {"matched": all_matched, "details": details}

        elif comparison_type == "exact" or comparison_type == "subset":
            # Row-by-row comparison
            if len(source_data) != len(target_data) and comparison_type == "exact":
                details.append(
                    ComparisonDetail(
                        comparison_type="row_count",
                        source_value=len(source_data),
                        target_value=len(target_data),
                        matched=False,
                        difference=f"Row count mismatch: {len(source_data)} vs {len(target_data)}",
                    )
                )
                return {"matched": False, "details": details}

            # Build lookup for target data if key columns specified
            if key_columns:
                target_lookup = {}
                for row in target_data:
                    key = tuple(row.get(k) for k in key_columns)
                    target_lookup[key] = row

                matched_count = 0
                mismatched_count = 0

                for source_row in source_data:
                    key = tuple(source_row.get(k) for k in key_columns)
                    target_row = target_lookup.get(key)

                    if not target_row:
                        mismatched_count += 1
                        details.append(
                            ComparisonDetail(
                                comparison_type="missing_row",
                                source_value=key,
                                matched=False,
                                row_key=str(key),
                                difference=f"Row with key {key} not found in target",
                            )
                        )
                        continue

                    # Compare columns
                    row_matched = True
                    cols_to_compare = comparison_columns or [
                        c for c in source_row.keys() if c not in key_columns
                    ]

                    for col in cols_to_compare:
                        if col in source_row and col in target_row:
                            col_matched = compare_values(
                                source_row[col], target_row[col], tolerance
                            )
                            if not col_matched:
                                row_matched = False
                                details.append(
                                    ComparisonDetail(
                                        comparison_type="value_mismatch",
                                        source_value=source_row[col],
                                        target_value=target_row[col],
                                        matched=False,
                                        column_name=col,
                                        row_key=str(key),
                                        difference=f"Mismatch in column {col} for key {key}",
                                    )
                                )

                    if row_matched:
                        matched_count += 1
                    else:
                        mismatched_count += 1

                all_matched = mismatched_count == 0
                return {"matched": all_matched, "details": details}

            else:
                # No key columns - compare row by row in order
                all_matched = True
                for i, (source_row, target_row) in enumerate(
                    zip(source_data, target_data)
                ):
                    cols_to_compare = comparison_columns or source_row.keys()
                    for col in cols_to_compare:
                        if col in source_row and col in target_row:
                            matched = compare_values(
                                source_row[col], target_row[col], tolerance
                            )
                            if not matched:
                                all_matched = False
                                details.append(
                                    ComparisonDetail(
                                        comparison_type="value_mismatch",
                                        source_value=source_row[col],
                                        target_value=target_row[col],
                                        matched=False,
                                        column_name=col,
                                        row_key=f"row_{i}",
                                        difference=f"Mismatch in column {col} at row {i}",
                                    )
                                )

                return {"matched": all_matched, "details": details}

        return {"matched": True, "details": details}

    async def execute_test_case(
        self,
        test_case: TestCase,
    ) -> dict[str, Any]:
        """
        Execute all queries for a test case.
        
        Args:
            test_case: Test case to execute
            
        Returns:
            Execution result with all proofs and comparisons
        """
        logger.info(f"Executing test case: {test_case.name}")
        start_time = time.time()

        all_proofs = []
        all_comparisons = []
        all_matched = True
        errors = []

        # Execute query pairs
        for query_pair in test_case.query_pairs:
            try:
                result = await self.execute_query_pair(query_pair)

                if result.get("source_proof"):
                    all_proofs.append(result["source_proof"])
                if result.get("target_proof"):
                    all_proofs.append(result["target_proof"])

                if result.get("comparison_details"):
                    all_comparisons.extend(result["comparison_details"])

                if not result.get("success") or not result.get("matched", True):
                    all_matched = False

                if result.get("error"):
                    errors.append(result["error"])

            except Exception as e:
                logger.error(f"Query pair {query_pair.id} failed: {e}")
                all_matched = False
                errors.append(str(e))

        # Execute standalone queries
        for query in test_case.standalone_queries:
            try:
                result = await self._execute_single_query(query)
                all_proofs.append(result["proof"])

                # Validate against expected result if specified
                if test_case.expected_result is not None:
                    if result["data"]:
                        # Compare first value with expected
                        actual = list(result["data"][0].values())[0] if result["data"][0] else None
                        matched = compare_values(actual, test_case.expected_result)
                        all_comparisons.append(
                            ComparisonDetail(
                                comparison_type="expected_value",
                                source_value=test_case.expected_result,
                                target_value=actual,
                                matched=matched,
                            )
                        )
                        if not matched:
                            all_matched = False
                    else:
                        all_matched = False

            except Exception as e:
                logger.error(f"Standalone query {query.id} failed: {e}")
                all_matched = False
                errors.append(str(e))

        duration = (time.time() - start_time) * 1000

        return {
            "test_case_id": test_case.id,
            "test_case_name": test_case.name,
            "passed": all_matched and not errors,
            "execution_proofs": all_proofs,
            "comparisons": all_comparisons,
            "errors": errors,
            "duration_ms": duration,
        }

    async def execute_test_cases_parallel(
        self,
        test_cases: list[TestCase],
        max_concurrent: int | None = None,
    ) -> list[dict[str, Any]]:
        """
        Execute multiple test cases with controlled parallelism.
        
        Args:
            test_cases: List of test cases to execute
            max_concurrent: Maximum concurrent test cases
            
        Returns:
            List of execution results
        """
        max_concurrent = max_concurrent or settings.max_parallel_workers
        semaphore = asyncio.Semaphore(max_concurrent)

        async def execute_with_semaphore(test_case: TestCase) -> dict[str, Any]:
            async with semaphore:
                return await self.execute_test_case(test_case)

        logger.info(
            f"Executing {len(test_cases)} test cases with max concurrency {max_concurrent}"
        )

        tasks = [execute_with_semaphore(tc) for tc in test_cases]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        processed_results = []
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                processed_results.append({
                    "test_case_id": test_cases[i].id,
                    "test_case_name": test_cases[i].name,
                    "passed": False,
                    "errors": [str(result)],
                    "execution_proofs": [],
                    "comparisons": [],
                })
            else:
                processed_results.append(result)

        return processed_results

    async def execute_raw_query(
        self,
        sql: str,
        database: str,
        timeout: int | None = None,
    ) -> dict[str, Any]:
        """
        Execute a raw SQL query for ad-hoc validation.
        
        Args:
            sql: SQL query to execute
            database: Database to execute on (source/target)
            timeout: Query timeout
            
        Returns:
            Query result with proof
        """
        query = ValidationQuery(
            id=f"adhoc_{generate_uuid()[:8]}",
            database=database,
            sql=sql,
            purpose="Ad-hoc query",
            timeout=timeout,
        )

        result = await self._execute_single_query(query)
        return {
            "success": True,
            "data": result["data"],
            "row_count": result["row_count"],
            "proof": result["proof"].model_dump(),
        }
