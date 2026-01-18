"""
Main validation engine that orchestrates the entire validation process.
Integrates LLM, database, and comparison components.
"""

import asyncio
import logging
import time
import uuid
from datetime import datetime
from typing import Any

from valagent.config import get_settings
from valagent.database import (
    DatabaseManager,
    ValidationRepository,
    get_db_manager,
)
from valagent.database.models import (
    TestCase,
    ValidationRun,
    ValidationStatus,
)
from valagent.llm import SQLGenerator, get_llm_client
from valagent.engine.executor import TestExecutor
from valagent.engine.comparator import DataComparator

logger = logging.getLogger(__name__)


class ValidationEngine:
    """
    Main orchestrator for ETL validation.
    Coordinates LLM analysis, SQL execution, and result comparison.
    """

    def __init__(
        self,
        db_manager: DatabaseManager | None = None,
        repository: ValidationRepository | None = None,
    ):
        self.settings = get_settings()
        self.db = db_manager or get_db_manager()
        self.repository = repository or ValidationRepository()
        self.executor = TestExecutor(self.db)
        self.comparator = DataComparator()
        self.sql_generator = SQLGenerator()
        self._source_schema: dict[str, Any] | None = None
        self._target_schema: dict[str, Any] | None = None

    async def initialize(self) -> None:
        """Initialize the engine and database connections."""
        await self.db.initialize()
        logger.info("Validation engine initialized")

    async def close(self) -> None:
        """Close database connections."""
        await self.db.close()
        logger.info("Validation engine closed")

    async def get_source_schema(self, refresh: bool = False) -> dict[str, Any]:
        """Get source database schema with caching."""
        if self._source_schema is None or refresh:
            self._source_schema = await asyncio.to_thread(
                self.db.get_source_schema
            )
        return self._source_schema

    async def get_target_schema(self, refresh: bool = False) -> dict[str, Any]:
        """Get target database schema with caching."""
        if self._target_schema is None or refresh:
            self._target_schema = await asyncio.to_thread(
                self.db.get_target_schema
            )
        return self._target_schema

    async def test_connections(self) -> dict[str, tuple[bool, str]]:
        """Test connections to both databases."""
        source_ok, source_msg = await self.db.test_connection("source")
        target_ok, target_msg = await self.db.test_connection("target")

        return {
            "source": (source_ok, source_msg),
            "target": (target_ok, target_msg),
        }

    async def analyze_business_rules(
        self,
        business_rules: list[str],
    ) -> dict[str, Any]:
        """
        Analyze business rules and generate validation plan.
        
        Args:
            business_rules: List of business rules in natural language
            
        Returns:
            Analysis with parsed rules and generated test cases
        """
        source_schema = await self.get_source_schema()
        target_schema = await self.get_target_schema()

        analysis = await self.sql_generator.analyze_business_rules(
            business_rules=business_rules,
            source_schema=source_schema,
            target_schema=target_schema,
        )

        return analysis

    async def create_validation_run(
        self,
        name: str,
        business_rules: list[str],
        description: str | None = None,
        created_by: str | None = None,
    ) -> ValidationRun:
        """
        Create a new validation run from business rules.
        Analyzes rules and generates test cases.
        """
        run_id = str(uuid.uuid4())
        logger.info(f"Creating validation run: {run_id}")

        # Analyze business rules to generate test cases
        analysis = await self.analyze_business_rules(business_rules)

        # Create test cases from analysis
        test_cases = []
        for test_data in analysis.get("test_cases", []):
            test_case = TestCase(
                id=str(uuid.uuid4()),
                name=test_data.get("name", "Unnamed Test"),
                description=test_data.get("description", ""),
                business_rule=test_data.get("business_rule", ""),
                source_query=test_data.get("source_query"),
                target_query=test_data.get("target_query", ""),
                validation_type=test_data.get("validation_type", "custom"),
                expected_result=test_data.get("expected_result"),
                status=ValidationStatus.PENDING,
            )
            test_cases.append(test_case)

        # Create validation run
        validation_run = ValidationRun(
            id=run_id,
            name=name,
            description=description,
            business_rules=business_rules,
            test_cases=test_cases,
            total_tests=len(test_cases),
            status=ValidationStatus.PENDING,
            created_by=created_by,
        )

        # Save to repository
        self.repository.save_validation_run(validation_run)
        for test_case in test_cases:
            self.repository.save_test_case(run_id, test_case)

        logger.info(f"Created validation run with {len(test_cases)} test cases")
        return validation_run

    async def execute_validation_run(
        self,
        run_id: str,
        parallel_tests: int = 5,
    ) -> ValidationRun:
        """
        Execute all test cases in a validation run.
        
        Args:
            run_id: ID of the validation run to execute
            parallel_tests: Maximum number of tests to run in parallel
        """
        validation_run = self.repository.get_validation_run(run_id)
        if not validation_run:
            raise ValueError(f"Validation run not found: {run_id}")

        logger.info(f"Executing validation run: {run_id}")
        start_time = time.perf_counter()

        # Update status to running
        validation_run.status = ValidationStatus.RUNNING
        validation_run.started_at = datetime.utcnow()
        self.repository.save_validation_run(validation_run)

        # Execute test cases
        semaphore = asyncio.Semaphore(parallel_tests)
        results = []

        async def run_test(test_case: TestCase) -> TestCase:
            async with semaphore:
                return await self._execute_test_case(test_case)

        # Run tests in parallel with controlled concurrency
        tasks = [run_test(tc) for tc in validation_run.test_cases]
        completed_tests = await asyncio.gather(*tasks, return_exceptions=True)

        # Process results
        passed = 0
        failed = 0
        errors = 0

        for i, result in enumerate(completed_tests):
            if isinstance(result, Exception):
                validation_run.test_cases[i].status = ValidationStatus.ERROR
                validation_run.test_cases[i].error_message = str(result)
                errors += 1
            else:
                validation_run.test_cases[i] = result
                if result.status == ValidationStatus.PASSED:
                    passed += 1
                elif result.status == ValidationStatus.FAILED:
                    failed += 1
                elif result.status == ValidationStatus.ERROR:
                    errors += 1

            # Save test case result
            self.repository.save_test_case(run_id, validation_run.test_cases[i])

        # Update run summary
        execution_time = (time.perf_counter() - start_time) * 1000
        validation_run.passed_tests = passed
        validation_run.failed_tests = failed
        validation_run.error_tests = errors
        validation_run.completed_at = datetime.utcnow()
        validation_run.execution_time_ms = execution_time

        if errors > 0:
            validation_run.status = ValidationStatus.ERROR
        elif failed > 0:
            validation_run.status = ValidationStatus.FAILED
        else:
            validation_run.status = ValidationStatus.PASSED

        self.repository.save_validation_run(validation_run)

        logger.info(
            f"Validation run completed: {passed} passed, {failed} failed, "
            f"{errors} errors in {execution_time:.0f}ms"
        )

        return validation_run

    async def _execute_test_case(self, test_case: TestCase) -> TestCase:
        """Execute a single test case."""
        logger.debug(f"Executing test case: {test_case.name}")
        start_time = time.perf_counter()

        try:
            test_case.status = ValidationStatus.RUNNING

            # Execute queries
            source_result = None
            target_result = None

            if test_case.source_query:
                source_result = await self.executor.execute_query(
                    test_case.source_query,
                    "source"
                )
                if not source_result.success:
                    raise Exception(f"Source query failed: {source_result.error}")

            if test_case.target_query:
                target_result = await self.executor.execute_query(
                    test_case.target_query,
                    "target"
                )
                if not target_result.success:
                    raise Exception(f"Target query failed: {target_result.error}")

            # Compare results based on validation type
            comparison = await self._compare_results(
                test_case.validation_type,
                source_result,
                target_result,
                test_case.expected_result,
            )

            # Determine pass/fail
            test_case.status = (
                ValidationStatus.PASSED if comparison.matches
                else ValidationStatus.FAILED
            )

            # Store evidence
            test_case.actual_result = {
                "source_row_count": comparison.source_row_count,
                "target_row_count": comparison.target_row_count,
                "matches": comparison.matches,
                "differences": comparison.differences[:10],  # Limit stored differences
                "difference_count": comparison.difference_count,
            }

            test_case.evidence = {
                "source_sample": source_result.rows[:5] if source_result else None,
                "target_sample": target_result.rows[:5] if target_result else None,
                "comparison_type": comparison.comparison_type,
                "execution_times": {
                    "source_ms": source_result.execution_time_ms if source_result else 0,
                    "target_ms": target_result.execution_time_ms if target_result else 0,
                },
            }

        except Exception as e:
            logger.error(f"Test case error: {test_case.name} - {e}")
            test_case.status = ValidationStatus.ERROR
            test_case.error_message = str(e)

        test_case.execution_time_ms = (time.perf_counter() - start_time) * 1000
        test_case.executed_at = datetime.utcnow()

        return test_case

    async def _compare_results(
        self,
        validation_type: str,
        source_result: Any,
        target_result: Any,
        expected_result: Any = None,
    ):
        """Compare results based on validation type."""
        from valagent.database.models import ComparisonResult

        if validation_type == "count":
            return self.comparator.compare_counts(source_result, target_result)

        elif validation_type == "aggregation":
            return self.comparator.compare_aggregations(source_result, target_result)

        elif validation_type == "data":
            # Determine key columns from result columns
            key_columns = target_result.columns[:1] if target_result else []
            return self.comparator.compare_data_rows(
                source_result,
                target_result,
                key_columns=key_columns,
            )

        elif validation_type == "hash":
            return self.comparator.compare_hashes(source_result, target_result)

        elif validation_type in ("custom", "referential"):
            # For custom validations, check if result is empty (no violations)
            return self.comparator.validate_target_only(
                target_result,
                "empty" if expected_result is None else "count_equals",
                expected_result,
            )

        else:
            # Default to count comparison
            if source_result:
                return self.comparator.compare_counts(source_result, target_result)
            else:
                return self.comparator.validate_target_only(
                    target_result,
                    "empty",
                )

    async def run_quick_validation(
        self,
        business_rules: list[str],
        name: str = "Quick Validation",
    ) -> ValidationRun:
        """
        Convenience method to create and execute a validation in one call.
        """
        run = await self.create_validation_run(
            name=name,
            business_rules=business_rules,
        )
        return await self.execute_validation_run(run.id)

    async def run_natural_language_query(
        self,
        query: str,
    ) -> dict[str, Any]:
        """
        Execute a natural language query against the databases.
        Converts NL to SQL and executes.
        """
        source_schema = await self.get_source_schema()
        target_schema = await self.get_target_schema()

        # Convert to SQL
        sql_result = await self.sql_generator.natural_language_to_sql(
            user_request=query,
            source_tables=list(source_schema.get("tables", {}).keys()),
            target_tables=list(target_schema.get("tables", {}).keys()),
        )

        # Execute generated queries
        results = {
            "understood_intent": sql_result.get("understood_intent"),
            "queries": [],
        }

        for query_info in sql_result.get("queries", []):
            sql = query_info.get("sql")
            if not sql:
                continue

            database = query_info.get("database", "target")
            if database == "both":
                # Execute on both
                source_result = await self.executor.execute_query(sql, "source")
                target_result = await self.executor.execute_query(sql, "target")
                results["queries"].append({
                    "purpose": query_info.get("purpose"),
                    "sql": sql,
                    "source_result": source_result.model_dump(),
                    "target_result": target_result.model_dump(),
                })
            else:
                result = await self.executor.execute_query(sql, database)
                results["queries"].append({
                    "purpose": query_info.get("purpose"),
                    "sql": sql,
                    "database": database,
                    "result": result.model_dump(),
                })

        return results

    async def get_schema_comparison(self) -> dict[str, Any]:
        """Compare source and target schemas."""
        source_schema = await self.get_source_schema()
        target_schema = await self.get_target_schema()

        return await self.sql_generator.compare_schemas(source_schema, target_schema)

    async def generate_standard_tests(
        self,
        focus_areas: list[str] | None = None,
    ) -> dict[str, Any]:
        """Generate standard ETL validation tests."""
        source_schema = await self.get_source_schema()
        target_schema = await self.get_target_schema()

        return await self.sql_generator.generate_comprehensive_tests(
            source_schema=source_schema,
            target_schema=target_schema,
            focus_areas=focus_areas,
        )

    def get_validation_run(self, run_id: str) -> ValidationRun | None:
        """Get a validation run by ID."""
        return self.repository.get_validation_run(run_id)

    def list_validation_runs(
        self,
        limit: int = 50,
        offset: int = 0,
        status: ValidationStatus | None = None,
    ) -> list[ValidationRun]:
        """List validation runs with optional filtering."""
        return self.repository.list_validation_runs(limit, offset, status)

    def get_statistics(self) -> dict[str, Any]:
        """Get overall validation statistics."""
        return self.repository.get_run_statistics()

    def delete_validation_run(self, run_id: str) -> bool:
        """Delete a validation run."""
        return self.repository.delete_validation_run(run_id)
