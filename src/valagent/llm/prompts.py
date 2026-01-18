"""
Prompt templates for the LLM-based validation agent.
Contains carefully crafted prompts for SQL generation and validation reasoning.
"""


class PromptTemplates:
    """Collection of prompt templates for the validation agent."""

    SYSTEM_PROMPT = """You are an expert Data Validation Agent specialized in ETL (Extract, Transform, Load) pipeline validation. Your role is to:

1. Analyze business rules expressed in natural language
2. Generate precise SQL queries to validate data transformations
3. Identify all possible test scenarios for comprehensive validation
4. Provide clear, actionable validation results

You have deep expertise in:
- PostgreSQL databases and SQL optimization
- Data quality assurance and integrity checking
- ETL pipeline validation patterns
- Statistical data comparison techniques

Always generate SQL that is:
- Safe (read-only, no modifications)
- Efficient (uses indexes, limits large result sets)
- Accurate (correctly implements the business logic)
- Well-commented (explains the validation purpose)

When generating test cases, consider:
- Row count validations
- Data completeness checks
- Transformation accuracy
- Referential integrity
- Null value handling
- Data type consistency
- Business rule compliance
- Edge cases and boundary conditions"""

    ANALYZE_BUSINESS_RULES = """Analyze the following business rules and generate a comprehensive validation plan.

## Database Schema Information

### Source Database (DB1):
{source_schema}

### Target Database (DB2):
{target_schema}

## Business Rules to Validate:
{business_rules}

## Your Task:
1. Parse each business rule and identify the validation requirements
2. Determine which tables and columns are involved
3. Identify the type of transformation being validated
4. Generate a list of specific test cases to validate each rule

Respond with a JSON object containing:
{{
    "parsed_rules": [
        {{
            "rule_id": "unique identifier",
            "original_rule": "the original business rule text",
            "interpretation": "your interpretation of what this rule means",
            "source_tables": ["list of source tables involved"],
            "target_tables": ["list of target tables involved"],
            "transformation_type": "type of transformation (e.g., mapping, aggregation, filter, join)",
            "validation_approach": "how to validate this rule"
        }}
    ],
    "test_cases": [
        {{
            "test_id": "unique test identifier",
            "rule_id": "which rule this tests",
            "name": "descriptive test name",
            "description": "what this test validates",
            "validation_type": "count|data|schema|aggregation|referential|custom",
            "priority": "critical|high|medium|low",
            "source_query": "SQL query for source database (or null)",
            "target_query": "SQL query for target database",
            "comparison_logic": "how to compare results",
            "expected_result": "what constitutes a pass"
        }}
    ],
    "summary": {{
        "total_rules": number,
        "total_test_cases": number,
        "tables_involved": ["list of all tables"],
        "risk_areas": ["potential issues to watch for"]
    }}
}}"""

    GENERATE_SQL_QUERIES = """Generate SQL queries to validate the following business rule.

## Business Rule:
{business_rule}

## Source Database Schema:
{source_schema}

## Target Database Schema:
{target_schema}

## Validation Type: {validation_type}

## Additional Context:
{context}

Generate SQL queries that will validate this business rule. The queries should:
1. Be read-only (SELECT statements only)
2. Be optimized for large datasets
3. Include appropriate LIMIT clauses for safety
4. Use CTEs for complex logic when beneficial
5. Include comments explaining the validation logic

Respond with a JSON object:
{{
    "source_query": "SQL query for source database (null if not needed)",
    "target_query": "SQL query for target database",
    "comparison_query": "optional SQL to compare results directly if both databases accessible",
    "explanation": "detailed explanation of what these queries validate",
    "expected_behavior": "what results indicate pass vs fail",
    "edge_cases_covered": ["list of edge cases this query handles"]
}}"""

    INTERPRET_RESULTS = """Analyze the validation results and provide a detailed assessment.

## Test Case Information:
- **Test Name**: {test_name}
- **Business Rule**: {business_rule}
- **Validation Type**: {validation_type}

## Queries Executed:
### Source Query:
```sql
{source_query}
```

### Target Query:
```sql
{target_query}
```

## Results:
### Source Result:
{source_result}

### Target Result:
{target_result}

## Execution Metrics:
- Source Query Time: {source_time_ms}ms
- Target Query Time: {target_time_ms}ms

Analyze these results and provide:

{{
    "status": "pass|fail|warning|error",
    "summary": "one-line summary of the result",
    "detailed_analysis": "comprehensive analysis of what the results show",
    "discrepancies": [
        {{
            "type": "type of discrepancy",
            "description": "detailed description",
            "severity": "critical|high|medium|low",
            "affected_records": "count or description of affected records"
        }}
    ],
    "recommendations": ["list of recommended actions if any issues found"],
    "confidence_score": 0.0 to 1.0,
    "evidence": {{
        "source_row_count": number,
        "target_row_count": number,
        "matching_percentage": percentage,
        "sample_differences": ["up to 5 example differences"]
    }}
}}"""

    GENERATE_COMPREHENSIVE_TESTS = """Based on the database schemas provided, generate a comprehensive set of validation tests that would typically be needed for an ETL validation.

## Source Database Schema:
{source_schema}

## Target Database Schema:
{target_schema}

## Focus Areas (if specified):
{focus_areas}

Generate standard ETL validation tests covering:
1. **Row Count Validations** - Ensure record counts match expectations
2. **Data Completeness** - Check for null values, required fields
3. **Referential Integrity** - Foreign key relationships are maintained
4. **Data Accuracy** - Values are correctly transformed
5. **Duplicate Detection** - No unintended duplicates
6. **Data Type Consistency** - Types match expectations
7. **Range Validations** - Values within expected ranges
8. **Aggregation Accuracy** - Sums, counts, averages match

Respond with a JSON object:
{{
    "validation_categories": [
        {{
            "category": "category name",
            "description": "what this category validates",
            "tests": [
                {{
                    "test_id": "unique identifier",
                    "name": "test name",
                    "description": "detailed description",
                    "validation_type": "count|data|schema|aggregation|referential|custom",
                    "source_query": "SQL query or null",
                    "target_query": "SQL query",
                    "expected_result": "description of expected outcome",
                    "priority": "critical|high|medium|low"
                }}
            ]
        }}
    ],
    "total_tests": number,
    "estimated_coverage": "percentage of typical ETL validations covered"
}}"""

    EXPLAIN_VALIDATION_FAILURE = """Explain why this validation test failed and suggest remediation steps.

## Failed Test:
- **Name**: {test_name}
- **Business Rule**: {business_rule}
- **Validation Type**: {validation_type}

## Expected:
{expected_result}

## Actual:
{actual_result}

## Query Details:
```sql
{query}
```

## Sample Discrepant Records (if available):
{sample_records}

Provide a detailed explanation:

{{
    "root_cause_analysis": "detailed analysis of why the validation failed",
    "possible_causes": [
        {{
            "cause": "description of possible cause",
            "likelihood": "high|medium|low",
            "investigation_steps": ["steps to verify this cause"]
        }}
    ],
    "impact_assessment": {{
        "severity": "critical|high|medium|low",
        "affected_scope": "description of what's affected",
        "business_impact": "potential business consequences"
    }},
    "remediation_steps": [
        {{
            "step": "remediation step",
            "responsibility": "who should do this",
            "priority": "immediate|short-term|long-term"
        }}
    ],
    "prevention_recommendations": ["how to prevent this in the future"]
}}"""

    NATURAL_LANGUAGE_TO_SQL = """Convert the following natural language request into SQL queries for data validation.

## User Request:
{user_request}

## Available Tables in Source Database:
{source_tables}

## Available Tables in Target Database:
{target_tables}

## Context:
- Source database contains the original/raw data
- Target database contains the transformed/loaded data
- We are validating ETL pipeline correctness

Generate appropriate SQL queries:

{{
    "understood_intent": "what you understood the user wants to validate",
    "queries": [
        {{
            "purpose": "what this query validates",
            "database": "source|target|both",
            "sql": "the SQL query",
            "expected_outcome": "what a successful result looks like"
        }}
    ],
    "clarifying_questions": ["any questions to better understand the request"],
    "assumptions_made": ["assumptions made in generating these queries"]
}}"""

    SCHEMA_COMPARISON = """Compare the source and target database schemas and identify any discrepancies or transformation patterns.

## Source Schema:
{source_schema}

## Target Schema:
{target_schema}

Analyze and report:

{{
    "schema_comparison": {{
        "matching_tables": ["tables that exist in both with same structure"],
        "renamed_tables": [
            {{"source": "source name", "target": "target name", "confidence": 0.0-1.0}}
        ],
        "new_tables": ["tables only in target"],
        "dropped_tables": ["tables only in source"],
        "modified_tables": [
            {{
                "table": "table name",
                "changes": [
                    {{"type": "column_added|column_removed|type_changed|renamed", "details": "specifics"}}
                ]
            }}
        ]
    }},
    "transformation_patterns": [
        {{
            "pattern": "denormalization|normalization|aggregation|filtering|merging",
            "description": "detailed description",
            "source_objects": ["involved source objects"],
            "target_objects": ["involved target objects"]
        }}
    ],
    "recommendations": ["recommendations for validation focus areas"],
    "risks": ["potential data quality risks based on schema changes"]
}}"""
