"""
Main Validation Agent.

The primary AI agent that orchestrates the entire validation workflow
with intelligent decision-making and adaptive test generation.
"""

import asyncio
from typing import Any, Callable, AsyncGenerator
from datetime import datetime

from ..core.database import db_manager
from ..core.config import settings
from ..services.validation_orchestrator import ValidationOrchestrator
from ..services.llm_service import llm_service
from ..services.schema_service import SchemaService
from ..models.results import ValidationReport, ResultStatus
from ..utils.logger import get_logger
from ..utils.helpers import generate_uuid, get_timestamp_str

logger = get_logger(__name__)


class ValidationAgent:
    """
    AI-powered validation agent for ETL data quality testing.
    
    This agent:
    1. Accepts business rules in natural language
    2. Analyzes source and target database schemas
    3. Generates comprehensive test cases
    4. Executes tests with parallel processing
    5. Analyzes results and generates detailed reports
    6. Provides actionable recommendations
    """

    def __init__(self):
        self._orchestrator: ValidationOrchestrator | None = None
        self._initialized = False
        self._current_session_id: str | None = None

    async def initialize(self) -> None:
        """Initialize the validation agent."""
        if self._initialized:
            return

        logger.info("Initializing Validation Agent...")

        self._orchestrator = ValidationOrchestrator(
            db_manager=db_manager,
            llm_service=llm_service,
        )
        await self._orchestrator.initialize()

        self._current_session_id = generate_uuid()[:12]
        self._initialized = True

        logger.info(f"Validation Agent initialized - Session: {self._current_session_id}")

    async def close(self) -> None:
        """Close the agent and release resources."""
        if self._orchestrator:
            await self._orchestrator.close()
        self._initialized = False
        logger.info("Validation Agent closed")

    async def validate(
        self,
        business_rules: str,
        validation_name: str | None = None,
        on_progress: Callable[[str, float], None] | None = None,
    ) -> ValidationReport:
        """
        Run validation with business rules.
        
        Args:
            business_rules: Natural language business rules
            validation_name: Optional name for this validation run
            on_progress: Optional callback for progress updates (message, percentage)
            
        Returns:
            Complete validation report
        """
        if not self._initialized:
            await self.initialize()

        def report_progress(message: str, percentage: float):
            logger.info(f"Progress: {percentage:.0f}% - {message}")
            if on_progress:
                on_progress(message, percentage)

        report_progress("Starting validation...", 0)

        try:
            report = await self._orchestrator.run_validation(
                business_rules_text=business_rules,
                validation_name=validation_name,
            )

            report_progress("Validation complete", 100)
            return report

        except Exception as e:
            logger.error(f"Validation failed: {e}")
            raise

    async def validate_streaming(
        self,
        business_rules: str,
        validation_name: str | None = None,
    ) -> AsyncGenerator[dict[str, Any], None]:
        """
        Run validation with streaming progress updates.
        
        Args:
            business_rules: Natural language business rules
            validation_name: Optional name for this validation run
            
        Yields:
            Progress updates and final report
        """
        if not self._initialized:
            await self.initialize()

        yield {
            "type": "progress",
            "message": "Initializing validation...",
            "percentage": 0,
        }

        try:
            # Step 1: Extract schemas
            yield {
                "type": "progress",
                "message": "Extracting database schemas...",
                "percentage": 10,
            }

            schema_service = SchemaService(db_manager)
            source_schema, target_schema = await schema_service.get_both_schemas()

            yield {
                "type": "schema_info",
                "source_tables": len(source_schema.tables),
                "target_tables": len(target_schema.tables),
                "percentage": 20,
            }

            # Step 2: Parse rules
            yield {
                "type": "progress",
                "message": "Parsing business rules...",
                "percentage": 30,
            }

            rule_set = await llm_service.parse_business_rules(
                natural_language_rules=business_rules,
                source_schema=source_schema,
                target_schema=target_schema,
            )

            yield {
                "type": "rules_parsed",
                "rule_count": len(rule_set.rules),
                "rules": [{"id": r.id, "name": r.name, "category": r.category.value} for r in rule_set.rules],
                "percentage": 40,
            }

            # Step 3: Generate test cases
            yield {
                "type": "progress",
                "message": "Generating test cases...",
                "percentage": 50,
            }

            all_test_cases = []
            for i, rule in enumerate(rule_set.rules):
                test_cases = await llm_service.generate_test_cases(
                    rule=rule,
                    source_schema=source_schema,
                    target_schema=target_schema,
                )
                all_test_cases.extend(test_cases)

                yield {
                    "type": "test_cases_generated",
                    "rule_id": rule.id,
                    "test_count": len(test_cases),
                    "total_tests": len(all_test_cases),
                    "percentage": 50 + (i + 1) / len(rule_set.rules) * 20,
                }

            # Step 4: Execute tests
            yield {
                "type": "progress",
                "message": f"Executing {len(all_test_cases)} test cases...",
                "percentage": 70,
            }

            from ..services.executor_service import QueryExecutorService
            executor = QueryExecutorService(db_manager)

            results = await executor.execute_test_cases_parallel(all_test_cases)

            passed = sum(1 for r in results if r.get("passed", False))
            failed = len(results) - passed

            yield {
                "type": "execution_complete",
                "total": len(results),
                "passed": passed,
                "failed": failed,
                "percentage": 90,
            }

            # Step 5: Generate report
            yield {
                "type": "progress",
                "message": "Generating report...",
                "percentage": 95,
            }

            report = await self._orchestrator.run_validation(
                business_rules_text=business_rules,
                validation_name=validation_name,
            )

            yield {
                "type": "complete",
                "report": report.to_json_summary(),
                "markdown": report.to_markdown(),
                "percentage": 100,
            }

        except Exception as e:
            logger.error(f"Streaming validation failed: {e}")
            yield {
                "type": "error",
                "message": str(e),
            }

    async def get_database_info(self) -> dict[str, Any]:
        """Get information about connected databases."""
        if not self._initialized:
            await self.initialize()

        source_info = await self._orchestrator.get_schema_info("source")
        target_info = await self._orchestrator.get_schema_info("target")

        return {
            "source": source_info,
            "target": target_info,
            "connection_status": "connected",
        }

    async def execute_query(
        self,
        query: str,
        database: str = "target",
    ) -> dict[str, Any]:
        """
        Execute an ad-hoc SQL query.
        
        Args:
            query: SQL query to execute
            database: Database to run on (source/target)
            
        Returns:
            Query result with execution proof
        """
        if not self._initialized:
            await self.initialize()

        return await self._orchestrator.execute_adhoc_query(query, database)

    async def generate_sql(
        self,
        description: str,
        database: str = "target",
    ) -> str:
        """
        Generate SQL from natural language description.
        
        Args:
            description: Natural language description
            database: Target database for context
            
        Returns:
            Generated SQL query
        """
        if not self._initialized:
            await self.initialize()

        schema_service = SchemaService(db_manager)
        if database == "source":
            schema = await schema_service.get_source_schema()
        else:
            schema = await schema_service.get_target_schema()

        return await llm_service.generate_sql_for_custom_check(
            description=description,
            database=database,
            schema=schema,
        )

    async def compare_databases(self) -> dict[str, Any]:
        """Compare source and target database schemas."""
        if not self._initialized:
            await self.initialize()

        return await self._orchestrator.compare_schemas()

    async def quick_validate(
        self,
        rule_description: str,
    ) -> dict[str, Any]:
        """
        Quick validation of a single rule.
        
        Args:
            rule_description: Single rule to validate
            
        Returns:
            Validation result
        """
        if not self._initialized:
            await self.initialize()

        report = await self.validate(
            business_rules=rule_description,
            validation_name="Quick Validation",
        )

        return {
            "status": report.overall_status.value,
            "passed": report.execution_summary.passed,
            "failed": report.execution_summary.failed,
            "message": report.ai_analysis or "Validation complete",
            "details": report.to_json_summary(),
        }


# Global agent instance
validation_agent = ValidationAgent()
