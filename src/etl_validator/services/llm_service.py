"""
LLM Service for AI-powered query generation and analysis.

Provides integration with OpenAI and other LLM providers for:
- Natural language to SQL translation
- Business rule parsing
- Test case generation
- Result analysis
"""

import json
import re
from typing import Any
import openai
from openai import AsyncOpenAI

from ..core.config import settings
from ..core.exceptions import LLMError, QueryGenerationError
from ..models.schema import DatabaseSchema
from ..models.rules import BusinessRule, BusinessRuleSet, RuleCategory, RulePriority
from ..models.test_case import (
    TestCase,
    TestCaseType,
    TestSuite,
    QueryPair,
    ValidationQuery,
)
from ..utils.logger import get_logger
from ..utils.helpers import generate_uuid, get_timestamp_str

logger = get_logger(__name__)


class LLMService:
    """
    Service for LLM interactions.
    
    Handles all AI-powered operations including query generation,
    rule parsing, and result analysis.
    """

    def __init__(self):
        self._client: AsyncOpenAI | None = None

    async def initialize(self) -> None:
        """Initialize the LLM client."""
        try:
            self._client = AsyncOpenAI(
                api_key=settings.openai_api_key.get_secret_value(),
            )
            logger.info(f"LLM Service initialized with model: {settings.openai_model}")
        except Exception as e:
            logger.error(f"Failed to initialize LLM client: {e}")
            raise LLMError(
                message="Failed to initialize LLM client",
                provider=settings.llm_provider,
                details={"error": str(e)},
            )

    def _extract_tables_from_sql(self, sql: str) -> set[str]:
        """
        Extract table names from a SQL query.
        
        Args:
            sql: SQL query string
            
        Returns:
            Set of table names (without schema prefix, lowercase)
        """
        tables = set()
        
        # Normalize SQL
        normalized = re.sub(r'\s+', ' ', sql.lower())
        
        # Pattern for FROM and JOIN clauses
        patterns = [
            r'\bfrom\s+(?:public\.)?(\w+)',
            r'\bjoin\s+(?:public\.)?(\w+)',
            r'\binto\s+(?:public\.)?(\w+)',
            r'\bupdate\s+(?:public\.)?(\w+)',
        ]
        
        for pattern in patterns:
            matches = re.findall(pattern, normalized)
            tables.update(matches)
        
        # Remove SQL keywords
        sql_keywords = {'select', 'where', 'and', 'or', 'on', 'as', 'in', 'not', 'null', 
                        'is', 'like', 'between', 'exists', 'case', 'when', 'then', 'else',
                        'end', 'group', 'order', 'by', 'having', 'limit', 'offset', 'union',
                        'intersect', 'except', 'values', 'set', 'true', 'false'}
        tables = tables - sql_keywords
        
        return tables
    
    def _validate_sql_tables(self, sql: str, valid_tables: set[str], db_name: str) -> bool:
        """
        Validate that all tables in the SQL exist in the valid tables set.
        
        Args:
            sql: SQL query to validate
            valid_tables: Set of valid table names (lowercase)
            db_name: Database name for logging
            
        Returns:
            True if valid, False otherwise
        """
        tables_in_query = self._extract_tables_from_sql(sql)
        
        if not tables_in_query:
            # Could not extract tables, assume valid
            return True
        
        invalid_tables = tables_in_query - valid_tables
        
        if invalid_tables:
            logger.warning(f"SQL for {db_name} references invalid tables: {invalid_tables}. Valid tables: {valid_tables}")
            return False
        
        return True

    async def _chat_completion(
        self,
        messages: list[dict[str, str]],
        temperature: float | None = None,
        max_tokens: int | None = None,
        response_format: dict | None = None,
    ) -> str:
        """Execute a chat completion request."""
        if not self._client:
            await self.initialize()

        try:
            kwargs = {
                "model": settings.openai_model,
                "messages": messages,
                "temperature": temperature or settings.openai_temperature,
                "max_tokens": max_tokens or settings.openai_max_tokens,
            }

            if response_format:
                kwargs["response_format"] = response_format

            response = await self._client.chat.completions.create(**kwargs)
            return response.choices[0].message.content

        except openai.RateLimitError as e:
            logger.error(f"LLM rate limit exceeded: {e}")
            raise LLMError(
                message="LLM rate limit exceeded",
                provider=settings.llm_provider,
                details={"error": str(e)},
            )
        except openai.APIError as e:
            logger.error(f"LLM API error: {e}")
            raise LLMError(
                message="LLM API error",
                provider=settings.llm_provider,
                details={"error": str(e)},
            )

    async def parse_business_rules(
        self,
        natural_language_rules: str,
        source_schema: DatabaseSchema,
        target_schema: DatabaseSchema,
    ) -> BusinessRuleSet:
        """
        Parse natural language business rules into structured format.
        
        Args:
            natural_language_rules: Business rules in plain English
            source_schema: Source database schema
            target_schema: Target database schema
            
        Returns:
            Structured BusinessRuleSet
        """
        system_prompt = """You are an expert data engineer specializing in ETL validation.
Your task is to parse natural language business rules and extract structured validation rules.

For each rule, identify:
1. The rule category (data_quality, data_completeness, data_transformation, data_consistency, referential_integrity, aggregation, business_logic, data_format, deduplication, null_handling, date_handling, numeric_handling, string_handling, custom)
2. Priority (critical, high, medium, low)
3. Source and target tables/columns involved
4. The transformation logic
5. Expected behavior for validation

Return a JSON object with the following structure:
{
    "rules": [
        {
            "name": "Rule name",
            "description": "Original rule description",
            "category": "category_name",
            "priority": "priority_level",
            "source_tables": ["schema.table1"],
            "source_columns": ["column1", "column2"],
            "target_tables": ["schema.table1"],
            "target_columns": ["column1", "column2"],
            "transformation_logic": "Description of transformation",
            "expected_behavior": "What should be validated"
        }
    ]
}"""

        user_prompt = f"""Parse the following business rules for ETL validation:

BUSINESS RULES:
{natural_language_rules}

SOURCE DATABASE SCHEMA:
{source_schema.to_llm_context(max_tables=30)}

TARGET DATABASE SCHEMA:
{target_schema.to_llm_context(max_tables=30)}

Extract all validation rules from the business rules text. Map them to the appropriate tables and columns in the schemas."""

        try:
            response = await self._chat_completion(
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt},
                ],
                response_format={"type": "json_object"},
            )

            parsed = json.loads(response)
            rules = []

            for i, rule_data in enumerate(parsed.get("rules", [])):
                rule = BusinessRule(
                    id=f"rule_{generate_uuid()[:8]}",
                    name=rule_data.get("name", f"Rule {i + 1}"),
                    description=rule_data.get("description", ""),
                    category=RuleCategory(rule_data.get("category", "custom")),
                    priority=RulePriority(rule_data.get("priority", "medium")),
                    source_tables=rule_data.get("source_tables", []),
                    source_columns=rule_data.get("source_columns", []),
                    target_tables=rule_data.get("target_tables", []),
                    target_columns=rule_data.get("target_columns", []),
                    transformation_logic=rule_data.get("transformation_logic"),
                    expected_behavior=rule_data.get("expected_behavior"),
                )
                rules.append(rule)

            rule_set = BusinessRuleSet(
                id=f"ruleset_{generate_uuid()[:8]}",
                name="Parsed Business Rules",
                description=f"Parsed from natural language input",
                rules=rules,
                created_at=get_timestamp_str(),
            )

            logger.info(f"Parsed {len(rules)} business rules from natural language")
            return rule_set

        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse LLM response as JSON: {e}")
            raise QueryGenerationError(
                message="Failed to parse business rules",
                details={"error": str(e)},
            )

    async def generate_test_cases(
        self,
        rule: BusinessRule,
        source_schema: DatabaseSchema,
        target_schema: DatabaseSchema,
    ) -> list[TestCase]:
        """
        Generate test cases for a business rule.
        
        Args:
            rule: Business rule to generate tests for
            source_schema: Source database schema
            target_schema: Target database schema
            
        Returns:
            List of generated test cases
        """
        # Get actual table names from each database
        source_table_names = list(source_schema.tables.keys())
        target_table_names = list(target_schema.tables.keys())
        
        # Find tables that exist in only one database (to explicitly warn about)
        source_only_tables = set(source_table_names) - set(target_table_names)
        target_only_tables = set(target_table_names) - set(source_table_names)
        
        system_prompt = f"""You are an expert QA engineer specializing in ETL/data validation testing.
Your task is to generate comprehensive test cases with SQL queries to validate business rules.

════════════════════════════════════════════════════════════════════════════════
CRITICAL DATABASE MAPPING - READ CAREFULLY
════════════════════════════════════════════════════════════════════════════════

SOURCE DATABASE contains EXACTLY these tables (and ONLY these):
  → {', '.join(sorted(source_table_names))}

TARGET DATABASE contains EXACTLY these tables (and ONLY these):
  → {', '.join(sorted(target_table_names))}

TABLES THAT EXIST ONLY IN SOURCE (DO NOT use in target_query):
  → {', '.join(sorted(source_only_tables)) if source_only_tables else 'None'}

TABLES THAT EXIST ONLY IN TARGET (DO NOT use in source_query):
  → {', '.join(sorted(target_only_tables)) if target_only_tables else 'None'}

════════════════════════════════════════════════════════════════════════════════
ABSOLUTE RULES - VIOLATIONS WILL CAUSE FAILURES
════════════════════════════════════════════════════════════════════════════════

1. source_query.sql MUST ONLY reference tables from SOURCE: {', '.join(sorted(source_table_names))}
2. target_query.sql MUST ONLY reference tables from TARGET: {', '.join(sorted(target_table_names))}
3. NEVER reference a source-only table in target_query (e.g., if 'inventory' is source-only, NEVER use it in target_query)
4. NEVER reference a target-only table in source_query
5. Before writing ANY query, verify each table name exists in the correct database list above

For each test case, generate:
1. Source query - to extract data from SOURCE database (using ONLY source tables listed above)
2. Target query - to validate transformed data in TARGET database (using ONLY target tables listed above)
3. Comparison logic - how to compare the results

GUIDELINES:
- Generate SQL queries compatible with PostgreSQL
- Use schema prefix 'public.' for all tables (e.g., public.inventory_status)
- Include appropriate JOINs for related tables WITHIN THE SAME DATABASE
- Handle NULL values properly
- Consider edge cases (empty results, large datasets)
- Generate queries that return comparable results

Return a JSON object:
{{
    "test_cases": [
        {{
            "name": "Test case name",
            "description": "What this test validates",
            "test_type": "row_count|data_match|aggregation|null_check|unique_check|transformation|format_validation|range_check|duplicate_check",
            "source_query": {{
                "sql": "SELECT ... FROM public.<source_table_name> ...",
                "purpose": "What this query extracts from SOURCE"
            }},
            "target_query": {{
                "sql": "SELECT ... FROM public.<target_table_name> ...",
                "purpose": "What this query validates in TARGET"
            }},
            "comparison_type": "exact|count|aggregate|subset",
            "comparison_columns": ["col1", "col2"],
            "key_columns": ["id_column"],
            "pass_criteria": "Description of pass condition"
        }}
    ]
}}"""

        # Build schema context for relevant tables
        source_tables_context = ""
        for table_name in rule.source_tables:
            if table_name in source_schema.tables:
                source_tables_context += source_schema.tables[table_name].to_ddl_summary() + "\n\n"

        target_tables_context = ""
        for table_name in rule.target_tables:
            if table_name in target_schema.tables:
                target_tables_context += target_schema.tables[table_name].to_ddl_summary() + "\n\n"

        # If no specific tables, provide overview
        if not source_tables_context:
            source_tables_context = source_schema.to_llm_context(max_tables=15)
        if not target_tables_context:
            target_tables_context = target_schema.to_llm_context(max_tables=15)

        user_prompt = f"""Generate test cases to validate the following business rule:

BUSINESS RULE:
{rule.to_prompt_context()}

=== SOURCE DATABASE (use ONLY these tables in source_query) ===
Available tables: {', '.join(source_table_names)}

Schema details:
{source_tables_context}

=== TARGET DATABASE (use ONLY these tables in target_query) ===
Available tables: {', '.join(target_table_names)}

Schema details:
{target_tables_context}

CRITICAL REMINDERS:
- source_query SQL must ONLY use tables from SOURCE DATABASE: {', '.join(source_table_names)}
- target_query SQL must ONLY use tables from TARGET DATABASE: {', '.join(target_table_names)}
- If a table exists in source but not target (or vice versa), adjust queries accordingly

Generate comprehensive test cases that validate this rule. Include:
1. Basic validation (row counts, data presence)
2. Transformation validation (if applicable)
3. Data quality checks (nulls, duplicates, formats)
4. Edge cases

Generate at most {settings.max_test_cases_per_rule} test cases for this rule."""

        try:
            response = await self._chat_completion(
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt},
                ],
                response_format={"type": "json_object"},
            )

            parsed = json.loads(response)
            test_cases = []
            
            # Create sets for validation (strip schema prefix for matching)
            source_table_set = set(
                t.lower().replace('public.', '') for t in source_table_names
            )
            target_table_set = set(
                t.lower().replace('public.', '') for t in target_table_names
            )

            for i, tc_data in enumerate(parsed.get("test_cases", [])):
                test_case_id = f"tc_{generate_uuid()[:8]}"

                # Create query pair
                query_pairs = []
                if tc_data.get("source_query") and tc_data.get("target_query"):
                    source_q = tc_data["source_query"]
                    target_q = tc_data["target_query"]
                    
                    source_sql = source_q.get("sql", "")
                    target_sql = target_q.get("sql", "")
                    
                    # Validate source query doesn't reference target-only tables
                    source_sql_valid = self._validate_sql_tables(source_sql, source_table_set, "source")
                    target_sql_valid = self._validate_sql_tables(target_sql, target_table_set, "target")
                    
                    if not source_sql_valid:
                        logger.warning(f"Skipping test case {tc_data.get('name', i)} - source query references invalid tables")
                        continue  # Skip this entire test case
                    if not target_sql_valid:
                        logger.warning(f"Skipping test case {tc_data.get('name', i)} - target query references invalid tables")
                        continue  # Skip this entire test case

                    query_pair = QueryPair(
                        id=f"qp_{generate_uuid()[:8]}",
                        source_query=ValidationQuery(
                            id=f"sq_{generate_uuid()[:8]}",
                            database="source",
                            sql=source_sql,
                            purpose=source_q.get("purpose", ""),
                        ),
                        target_query=ValidationQuery(
                            id=f"tq_{generate_uuid()[:8]}",
                            database="target",
                            sql=target_sql,
                            purpose=target_q.get("purpose", ""),
                        ),
                        comparison_type=tc_data.get("comparison_type", "exact"),
                        comparison_columns=tc_data.get("comparison_columns", []),
                        key_columns=tc_data.get("key_columns", []),
                    )
                    query_pairs.append(query_pair)
                else:
                    # No query pair, skip this test case
                    logger.warning(f"Skipping test case {tc_data.get('name', i)} - missing source or target query")
                    continue

                # Map test type
                test_type_str = tc_data.get("test_type", "custom")
                try:
                    test_type = TestCaseType(test_type_str)
                except ValueError:
                    test_type = TestCaseType.CUSTOM

                test_case = TestCase(
                    id=test_case_id,
                    name=tc_data.get("name", f"Test Case {i + 1}"),
                    description=tc_data.get("description", ""),
                    rule_id=rule.id,
                    test_type=test_type,
                    query_pairs=query_pairs,
                    pass_criteria=tc_data.get("pass_criteria"),
                    priority=i + 1,
                )
                test_cases.append(test_case)

            logger.info(f"Generated {len(test_cases)} test cases for rule {rule.id}")
            return test_cases

        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse test case response: {e}")
            raise QueryGenerationError(
                message="Failed to generate test cases",
                rule=rule.name,
                details={"error": str(e)},
            )

    async def analyze_validation_results(
        self,
        test_results: list[dict[str, Any]],
        business_rules: BusinessRuleSet,
        source_schema: DatabaseSchema,
        target_schema: DatabaseSchema,
    ) -> dict[str, Any]:
        """
        Analyze validation results and generate insights.
        
        Args:
            test_results: List of test result summaries
            business_rules: Original business rules
            source_schema: Source database schema
            target_schema: Target database schema
            
        Returns:
            Analysis with insights and recommendations
        """
        system_prompt = """You are an expert data quality analyst reviewing ETL validation results.
Analyze the test results and provide:
1. Overall assessment of data quality
2. Root cause analysis for failures
3. Specific recommendations for fixes
4. Coverage analysis - what scenarios were tested
5. Risk assessment

Be specific and actionable in your recommendations."""

        # Prepare results summary
        results_summary = json.dumps(test_results, indent=2, default=str)

        user_prompt = f"""Analyze the following ETL validation results:

BUSINESS RULES:
{json.dumps([r.model_dump() for r in business_rules.rules], indent=2, default=str)[:3000]}

TEST RESULTS:
{results_summary[:5000]}

Provide:
1. Executive summary
2. Detailed analysis of failures (if any)
3. Scenarios that were covered
4. Recommendations for fixing issues
5. Suggestions for additional tests

Format your response as JSON:
{{
    "executive_summary": "Overall assessment",
    "overall_status": "pass|fail|partial",
    "scenarios_covered": [
        {{"name": "Scenario", "description": "What was tested", "covered": true}}
    ],
    "failure_analysis": [
        {{"test_name": "Test", "root_cause": "Cause", "impact": "Impact description"}}
    ],
    "recommendations": ["Recommendation 1", "Recommendation 2"],
    "additional_tests_suggested": ["Suggested test 1"],
    "risk_level": "low|medium|high|critical"
}}"""

        try:
            response = await self._chat_completion(
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt},
                ],
                response_format={"type": "json_object"},
                max_tokens=4096,
            )

            analysis = json.loads(response)
            logger.info("Generated validation analysis")
            return analysis

        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse analysis response: {e}")
            return {
                "executive_summary": "Failed to generate detailed analysis",
                "overall_status": "unknown",
                "recommendations": [],
                "error": str(e),
            }

    async def generate_sql_for_custom_check(
        self,
        description: str,
        database: str,
        schema: DatabaseSchema,
    ) -> str:
        """
        Generate SQL query from natural language description.
        
        Args:
            description: Natural language description of the query
            database: Which database (source/target)
            schema: Database schema
            
        Returns:
            Generated SQL query
        """
        system_prompt = """You are an expert SQL developer. Generate a PostgreSQL query based on the description.
Return ONLY the SQL query, no explanations. The query should be optimized and handle edge cases.
Use proper PostgreSQL syntax."""

        user_prompt = f"""Generate a SQL query for the following:

DESCRIPTION:
{description}

DATABASE SCHEMA:
{schema.to_llm_context(max_tables=20)}

Return only the SQL query."""

        try:
            response = await self._chat_completion(
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt},
                ],
                temperature=0.0,
            )

            # Clean up the response
            sql = response.strip()
            if sql.startswith("```sql"):
                sql = sql[6:]
            if sql.startswith("```"):
                sql = sql[3:]
            if sql.endswith("```"):
                sql = sql[:-3]

            return sql.strip()

        except Exception as e:
            logger.error(f"Failed to generate SQL: {e}")
            raise QueryGenerationError(
                message="Failed to generate SQL query",
                details={"description": description, "error": str(e)},
            )


# Global LLM service instance
llm_service = LLMService()
