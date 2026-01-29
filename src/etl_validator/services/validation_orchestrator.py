"""
Validation Orchestrator - Main orchestration service.

Coordinates the entire validation workflow from business rules
to final report generation.
"""

import asyncio
import time
from typing import Any

from ..core.database import DatabaseManager, db_manager
from ..core.config import settings
from ..models.rules import BusinessRuleSet, BusinessRule
from ..models.test_case import TestCase, TestCaseStatus, TestSuite
from ..models.results import (
    TestResult,
    TestExecutionSummary,
    ValidationReport,
    ResultStatus,
    ScenarioCoverage,
    ExecutionProof,
)
from .schema_service import SchemaService
from .llm_service import LLMService, llm_service
from .executor_service import QueryExecutorService
from ..utils.logger import get_logger
from ..utils.helpers import generate_uuid, get_timestamp_str

logger = get_logger(__name__)


class ValidationOrchestrator:
    """
    Main orchestration service for ETL validation.
    
    Coordinates:
    1. Schema extraction from source and target databases
    2. Business rule parsing from natural language
    3. Test case generation
    4. Parallel test execution
    5. Result analysis and report generation
    """

    def __init__(
        self,
        db_manager: DatabaseManager | None = None,
        llm_service: LLMService | None = None,
    ):
        self._db_manager = db_manager
        self._llm_service = llm_service or llm_service
        self._schema_service: SchemaService | None = None
        self._executor_service: QueryExecutorService | None = None
        self._initialized = False

    async def initialize(self) -> None:
        """Initialize all services."""
        if self._initialized:
            return

        logger.info("Initializing Validation Orchestrator...")

        # Use global db_manager if not provided
        if not self._db_manager:
            from ..core.database import db_manager as global_db_manager
            self._db_manager = global_db_manager

        # Initialize database connections
        await self._db_manager.initialize()

        # Initialize services
        self._schema_service = SchemaService(self._db_manager)
        self._executor_service = QueryExecutorService(self._db_manager)

        # Initialize LLM service
        if not self._llm_service:
            from .llm_service import llm_service as global_llm_service
            self._llm_service = global_llm_service
        await self._llm_service.initialize()

        self._initialized = True
        logger.info("Validation Orchestrator initialized successfully")

    async def close(self) -> None:
        """Close all connections."""
        if self._db_manager:
            await self._db_manager.close()
        self._initialized = False
        logger.info("Validation Orchestrator closed")

    async def run_validation(
        self,
        business_rules_text: str,
        validation_name: str | None = None,
    ) -> ValidationReport:
        """
        Run complete validation workflow.
        
        Args:
            business_rules_text: Natural language business rules
            validation_name: Optional name for the validation run
            
        Returns:
            Complete validation report
        """
        if not self._initialized:
            await self.initialize()

        run_id = generate_uuid()[:12]
        validation_name = validation_name or f"Validation Run {run_id}"

        logger.info(f"Starting validation run: {validation_name}")
        start_time = time.time()

        try:
            # Step 1: Extract schemas
            logger.info("Step 1: Extracting database schemas...")
            source_schema, target_schema = await self._schema_service.get_both_schemas()
            logger.info(
                f"Schemas extracted - Source: {len(source_schema.tables)} tables, "
                f"Target: {len(target_schema.tables)} tables"
            )

            # Configure executor with valid table names (strip schema prefix for matching)
            source_table_names = set(
                t.replace('public.', '') for t in source_schema.tables.keys()
            )
            target_table_names = set(
                t.replace('public.', '') for t in target_schema.tables.keys()
            )
            self._executor_service.set_schema_tables(source_table_names, target_table_names)

            # Step 2: Parse business rules
            logger.info("Step 2: Parsing business rules...")
            rule_set = await self._llm_service.parse_business_rules(
                natural_language_rules=business_rules_text,
                source_schema=source_schema,
                target_schema=target_schema,
            )
            logger.info(f"Parsed {len(rule_set.rules)} business rules")

            # Step 3: Generate test cases for each rule
            logger.info("Step 3: Generating test cases...")
            all_test_cases = []
            for rule in rule_set.rules:
                test_cases = await self._llm_service.generate_test_cases(
                    rule=rule,
                    source_schema=source_schema,
                    target_schema=target_schema,
                )
                all_test_cases.extend(test_cases)
            logger.info(f"Generated {len(all_test_cases)} test cases")

            # Step 4: Execute test cases
            logger.info("Step 4: Executing test cases...")
            execution_results = await self._executor_service.execute_test_cases_parallel(
                test_cases=all_test_cases,
            )

            # Step 5: Build test results
            logger.info("Step 5: Building test results...")
            test_results = self._build_test_results(execution_results, rule_set)

            # Step 6: Analyze results with AI
            logger.info("Step 6: Analyzing results...")
            analysis = await self._llm_service.analyze_validation_results(
                test_results=[r.to_summary() for r in test_results],
                business_rules=rule_set,
                source_schema=source_schema,
                target_schema=target_schema,
            )

            # Step 7: Build final report
            logger.info("Step 7: Generating report...")
            total_duration = (time.time() - start_time) * 1000
            report = self._build_report(
                run_id=run_id,
                validation_name=validation_name,
                rule_set=rule_set,
                test_results=test_results,
                analysis=analysis,
                total_duration=total_duration,
            )

            logger.info(
                f"Validation complete: {report.execution_summary.passed}/{report.execution_summary.total_tests} passed "
                f"({report.execution_summary.pass_rate:.1f}%) in {total_duration:.0f}ms"
            )

            return report

        except Exception as e:
            logger.error(f"Validation failed: {e}")
            raise

    def _build_test_results(
        self,
        execution_results: list[dict[str, Any]],
        rule_set: BusinessRuleSet,
    ) -> list[TestResult]:
        """Build TestResult objects from execution results."""
        test_results = []

        # Create rule lookup
        rule_lookup = {r.id: r for r in rule_set.rules}

        for result in execution_results:
            test_case_id = result.get("test_case_id", "unknown")
            test_case_name = result.get("test_case_name", "Unknown Test")
            passed = result.get("passed", False)
            errors = result.get("errors", [])

            # Determine status
            if errors:
                status = ResultStatus.ERROR
            elif passed:
                status = ResultStatus.PASSED
            else:
                status = ResultStatus.FAILED

            # Build execution proofs
            execution_proofs = []
            for proof_data in result.get("execution_proofs", []):
                if isinstance(proof_data, ExecutionProof):
                    execution_proofs.append(proof_data)
                elif isinstance(proof_data, dict):
                    execution_proofs.append(ExecutionProof(**proof_data))

            # Build comparisons
            comparisons = result.get("comparisons", [])

            # Extract row counts
            source_row_count = None
            target_row_count = None
            for proof in execution_proofs:
                if proof.database == "source":
                    source_row_count = proof.row_count
                elif proof.database == "target":
                    target_row_count = proof.row_count

            # Build message
            if passed:
                message = "All validations passed"
            elif errors:
                message = f"Execution error: {errors[0]}"
            else:
                # Find mismatches
                mismatches = [c for c in comparisons if hasattr(c, 'matched') and not c.matched]
                if mismatches:
                    message = f"Validation failed: {len(mismatches)} mismatches found"
                else:
                    message = "Validation failed"

            test_result = TestResult(
                test_case_id=test_case_id,
                test_case_name=test_case_name,
                rule_id=test_case_id.split("_")[0] if "_" in test_case_id else "unknown",
                status=status,
                started_at=get_timestamp_str(),
                completed_at=get_timestamp_str(),
                duration_ms=result.get("duration_ms", 0),
                execution_proofs=execution_proofs,
                comparisons=comparisons,
                message=message,
                source_row_count=source_row_count,
                target_row_count=target_row_count,
            )
            test_results.append(test_result)

        return test_results

    def _build_report(
        self,
        run_id: str,
        validation_name: str,
        rule_set: BusinessRuleSet,
        test_results: list[TestResult],
        analysis: dict[str, Any],
        total_duration: float,
    ) -> ValidationReport:
        """Build the final validation report."""
        # Calculate summary
        passed = sum(1 for r in test_results if r.status == ResultStatus.PASSED)
        failed = sum(1 for r in test_results if r.status == ResultStatus.FAILED)
        errors = sum(1 for r in test_results if r.status == ResultStatus.ERROR)
        skipped = sum(1 for r in test_results if r.status == ResultStatus.SKIPPED)
        total = len(test_results)

        pass_rate = (passed / total * 100) if total > 0 else 0
        avg_duration = (
            sum(r.duration_ms for r in test_results) / total if total > 0 else 0
        )

        execution_summary = TestExecutionSummary(
            total_tests=total,
            passed=passed,
            failed=failed,
            errors=errors,
            skipped=skipped,
            total_duration_ms=total_duration,
            average_duration_ms=avg_duration,
            pass_rate=pass_rate,
            critical_failures=sum(
                1
                for r in test_results
                if r.status == ResultStatus.FAILED
            ),
        )

        # Determine overall status
        if errors > 0:
            overall_status = ResultStatus.ERROR
        elif failed > 0:
            overall_status = ResultStatus.FAILED if failed > passed else ResultStatus.PARTIAL
        else:
            overall_status = ResultStatus.PASSED

        # Build scenario coverage
        scenarios = []
        for scenario in analysis.get("scenarios_covered", []):
            scenarios.append(
                ScenarioCoverage(
                    scenario_name=scenario.get("name", "Unknown"),
                    description=scenario.get("description", ""),
                    covered=scenario.get("covered", False),
                    test_case_ids=[],
                )
            )

        return ValidationReport(
            report_id=run_id,
            report_name=validation_name,
            generated_at=get_timestamp_str(),
            source_database="source",
            target_database="target",
            rule_set_id=rule_set.id,
            rule_set_name=rule_set.name,
            overall_status=overall_status,
            execution_summary=execution_summary,
            test_results=test_results,
            scenarios_covered=scenarios,
            ai_analysis=analysis.get("executive_summary"),
            ai_recommendations=analysis.get("recommendations", []),
            execution_metadata={
                "total_rules": len(rule_set.rules),
                "risk_level": analysis.get("risk_level", "unknown"),
            },
        )

    async def execute_adhoc_query(
        self,
        sql: str,
        database: str,
    ) -> dict[str, Any]:
        """Execute an ad-hoc SQL query."""
        if not self._initialized:
            await self.initialize()

        return await self._executor_service.execute_raw_query(sql, database)

    async def get_schema_info(self, database: str) -> dict[str, Any]:
        """Get schema information for a database."""
        if not self._initialized:
            await self.initialize()

        if database == "source":
            schema = await self._schema_service.get_source_schema()
        else:
            schema = await self._schema_service.get_target_schema()

        return {
            "database": schema.database_name,
            "tables": len(schema.tables),
            "schema": {
                name: {
                    "columns": len(table.columns),
                    "primary_keys": table.primary_keys,
                    "row_count": table.approximate_row_count,
                }
                for name, table in schema.tables.items()
            },
        }

    async def compare_schemas(self) -> dict[str, Any]:
        """Compare source and target schemas."""
        if not self._initialized:
            await self.initialize()

        comparison = await self._schema_service.compare_schemas()
        return comparison.model_dump()


# Global orchestrator instance
orchestrator = ValidationOrchestrator()
