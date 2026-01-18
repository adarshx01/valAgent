"""
SQL Generator using LLM for converting business rules to SQL queries.
"""

import json
import logging
from typing import Any

from valagent.llm.client import LLMClient, get_llm_client
from valagent.llm.prompts import PromptTemplates

logger = logging.getLogger(__name__)


class SQLGenerator:
    """
    Generates SQL queries from natural language business rules using LLM.
    Includes validation and safety checks on generated SQL.
    """

    def __init__(self, llm_client: LLMClient | None = None):
        self.llm = llm_client or get_llm_client()
        self.prompts = PromptTemplates()

        # SQL safety patterns - queries must be read-only
        self.forbidden_patterns = [
            "INSERT",
            "UPDATE",
            "DELETE",
            "DROP",
            "TRUNCATE",
            "ALTER",
            "CREATE",
            "GRANT",
            "REVOKE",
            "EXECUTE",
            "EXEC",
            "INTO",  # SELECT INTO
        ]

    def _validate_sql_safety(self, sql: str) -> tuple[bool, str]:
        """
        Validate that SQL is safe (read-only).
        Returns (is_safe, error_message).
        """
        if not sql:
            return True, ""

        sql_upper = sql.upper()

        # Check for forbidden patterns
        for pattern in self.forbidden_patterns:
            # Match whole words only
            import re

            if re.search(rf"\b{pattern}\b", sql_upper):
                return False, f"Unsafe SQL: Contains forbidden keyword '{pattern}'"

        # Must start with SELECT, WITH, or be a comment
        sql_stripped = sql.strip()
        valid_starts = ("SELECT", "WITH", "--", "/*")
        if not any(sql_stripped.upper().startswith(s) for s in valid_starts):
            return False, "SQL must start with SELECT or WITH"

        return True, ""

    def _format_schema_for_prompt(self, schema: dict[str, Any]) -> str:
        """Format database schema information for LLM prompt."""
        lines = []

        for table_name, table_info in schema.get("tables", {}).items():
            lines.append(f"\n### Table: {table_name}")

            # Columns
            lines.append("Columns:")
            for col in table_info.get("columns", []):
                pk_marker = " [PK]" if col.get("primary_key") else ""
                nullable = "" if col.get("nullable", True) else " NOT NULL"
                lines.append(
                    f"  - {col['name']}: {col['type']}{pk_marker}{nullable}"
                )

            # Foreign keys
            fks = table_info.get("foreign_keys", [])
            if fks:
                lines.append("Foreign Keys:")
                for fk in fks:
                    lines.append(
                        f"  - {fk['columns']} -> {fk['referred_table']}({fk['referred_columns']})"
                    )

            # Row count if available
            if table_info.get("row_count"):
                lines.append(f"Approximate Row Count: {table_info['row_count']:,}")

        # Views
        views = schema.get("views", [])
        if views:
            lines.append("\n### Views:")
            for view in views:
                lines.append(f"  - {view}")

        return "\n".join(lines)

    async def analyze_business_rules(
        self,
        business_rules: list[str],
        source_schema: dict[str, Any],
        target_schema: dict[str, Any],
    ) -> dict[str, Any]:
        """
        Analyze business rules and generate a validation plan with test cases.
        """
        prompt = self.prompts.ANALYZE_BUSINESS_RULES.format(
            source_schema=self._format_schema_for_prompt(source_schema),
            target_schema=self._format_schema_for_prompt(target_schema),
            business_rules="\n".join(f"- {rule}" for rule in business_rules),
        )

        try:
            result = await self.llm.generate_json(
                prompt=prompt,
                system_prompt=self.prompts.SYSTEM_PROMPT,
            )

            # Validate generated SQL in test cases
            for test_case in result.get("test_cases", []):
                for query_field in ["source_query", "target_query"]:
                    query = test_case.get(query_field)
                    if query:
                        is_safe, error = self._validate_sql_safety(query)
                        if not is_safe:
                            logger.warning(
                                f"Unsafe SQL generated for {test_case.get('test_id')}: {error}"
                            )
                            test_case[query_field] = None
                            test_case["safety_error"] = error

            return result

        except Exception as e:
            logger.error(f"Error analyzing business rules: {e}")
            raise

    async def generate_sql_for_rule(
        self,
        business_rule: str,
        source_schema: dict[str, Any],
        target_schema: dict[str, Any],
        validation_type: str = "data",
        context: str = "",
    ) -> dict[str, Any]:
        """
        Generate SQL queries to validate a specific business rule.
        """
        prompt = self.prompts.GENERATE_SQL_QUERIES.format(
            business_rule=business_rule,
            source_schema=self._format_schema_for_prompt(source_schema),
            target_schema=self._format_schema_for_prompt(target_schema),
            validation_type=validation_type,
            context=context or "No additional context provided.",
        )

        try:
            result = await self.llm.generate_json(
                prompt=prompt,
                system_prompt=self.prompts.SYSTEM_PROMPT,
            )

            # Validate SQL safety
            for query_field in ["source_query", "target_query", "comparison_query"]:
                query = result.get(query_field)
                if query:
                    is_safe, error = self._validate_sql_safety(query)
                    if not is_safe:
                        logger.warning(f"Unsafe SQL in {query_field}: {error}")
                        result[query_field] = None
                        result["safety_warnings"] = result.get("safety_warnings", [])
                        result["safety_warnings"].append(
                            f"{query_field}: {error}"
                        )

            return result

        except Exception as e:
            logger.error(f"Error generating SQL for rule: {e}")
            raise

    async def interpret_results(
        self,
        test_name: str,
        business_rule: str,
        validation_type: str,
        source_query: str | None,
        target_query: str,
        source_result: Any,
        target_result: Any,
        source_time_ms: float = 0,
        target_time_ms: float = 0,
    ) -> dict[str, Any]:
        """
        Interpret validation results and provide detailed analysis.
        """
        prompt = self.prompts.INTERPRET_RESULTS.format(
            test_name=test_name,
            business_rule=business_rule,
            validation_type=validation_type,
            source_query=source_query or "N/A",
            target_query=target_query,
            source_result=json.dumps(source_result, indent=2, default=str)
            if source_result
            else "N/A",
            target_result=json.dumps(target_result, indent=2, default=str),
            source_time_ms=source_time_ms,
            target_time_ms=target_time_ms,
        )

        try:
            return await self.llm.generate_json(
                prompt=prompt,
                system_prompt=self.prompts.SYSTEM_PROMPT,
            )
        except Exception as e:
            logger.error(f"Error interpreting results: {e}")
            raise

    async def generate_comprehensive_tests(
        self,
        source_schema: dict[str, Any],
        target_schema: dict[str, Any],
        focus_areas: list[str] | None = None,
    ) -> dict[str, Any]:
        """
        Generate a comprehensive set of validation tests based on schemas.
        """
        prompt = self.prompts.GENERATE_COMPREHENSIVE_TESTS.format(
            source_schema=self._format_schema_for_prompt(source_schema),
            target_schema=self._format_schema_for_prompt(target_schema),
            focus_areas="\n".join(f"- {area}" for area in focus_areas)
            if focus_areas
            else "No specific focus areas - generate comprehensive tests.",
        )

        try:
            result = await self.llm.generate_json(
                prompt=prompt,
                system_prompt=self.prompts.SYSTEM_PROMPT,
            )

            # Validate all generated SQL
            for category in result.get("validation_categories", []):
                for test in category.get("tests", []):
                    for query_field in ["source_query", "target_query"]:
                        query = test.get(query_field)
                        if query:
                            is_safe, error = self._validate_sql_safety(query)
                            if not is_safe:
                                test[query_field] = None
                                test["safety_warning"] = error

            return result

        except Exception as e:
            logger.error(f"Error generating comprehensive tests: {e}")
            raise

    async def natural_language_to_sql(
        self,
        user_request: str,
        source_tables: list[str],
        target_tables: list[str],
    ) -> dict[str, Any]:
        """
        Convert a natural language request to SQL queries.
        """
        prompt = self.prompts.NATURAL_LANGUAGE_TO_SQL.format(
            user_request=user_request,
            source_tables="\n".join(f"- {t}" for t in source_tables),
            target_tables="\n".join(f"- {t}" for t in target_tables),
        )

        try:
            result = await self.llm.generate_json(
                prompt=prompt,
                system_prompt=self.prompts.SYSTEM_PROMPT,
            )

            # Validate SQL safety
            for query_info in result.get("queries", []):
                query = query_info.get("sql")
                if query:
                    is_safe, error = self._validate_sql_safety(query)
                    if not is_safe:
                        query_info["sql"] = None
                        query_info["safety_error"] = error

            return result

        except Exception as e:
            logger.error(f"Error converting natural language to SQL: {e}")
            raise

    async def compare_schemas(
        self,
        source_schema: dict[str, Any],
        target_schema: dict[str, Any],
    ) -> dict[str, Any]:
        """
        Compare source and target schemas to identify transformation patterns.
        """
        prompt = self.prompts.SCHEMA_COMPARISON.format(
            source_schema=self._format_schema_for_prompt(source_schema),
            target_schema=self._format_schema_for_prompt(target_schema),
        )

        try:
            return await self.llm.generate_json(
                prompt=prompt,
                system_prompt=self.prompts.SYSTEM_PROMPT,
            )
        except Exception as e:
            logger.error(f"Error comparing schemas: {e}")
            raise

    async def explain_failure(
        self,
        test_name: str,
        business_rule: str,
        validation_type: str,
        expected_result: Any,
        actual_result: Any,
        query: str,
        sample_records: list[dict] | None = None,
    ) -> dict[str, Any]:
        """
        Explain why a validation failed and suggest remediation.
        """
        prompt = self.prompts.EXPLAIN_VALIDATION_FAILURE.format(
            test_name=test_name,
            business_rule=business_rule,
            validation_type=validation_type,
            expected_result=json.dumps(expected_result, indent=2, default=str),
            actual_result=json.dumps(actual_result, indent=2, default=str),
            query=query,
            sample_records=json.dumps(sample_records[:5], indent=2, default=str)
            if sample_records
            else "No sample records available",
        )

        try:
            return await self.llm.generate_json(
                prompt=prompt,
                system_prompt=self.prompts.SYSTEM_PROMPT,
            )
        except Exception as e:
            logger.error(f"Error explaining failure: {e}")
            raise
