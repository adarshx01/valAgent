"""
API route handlers for ValAgent.
"""

import logging
from datetime import datetime
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query, BackgroundTasks
from fastapi.responses import JSONResponse

from valagent import __version__
from valagent.config import get_settings
from valagent.database.models import ValidationStatus
from valagent.engine import ValidationEngine
from valagent.api.schemas import (
    HealthResponse,
    ConnectionsResponse,
    ConnectionStatus,
    SchemaResponse,
    SchemaComparisonResponse,
    BusinessRulesRequest,
    CreateValidationRequest,
    ExecuteValidationRequest,
    QuickValidationRequest,
    ValidationRunResponse,
    ValidationRunListResponse,
    ValidationStatisticsResponse,
    TestCaseResponse,
    NaturalLanguageQueryRequest,
    NaturalLanguageQueryResponse,
    QueryExecutionRequest,
    QueryResultResponse,
    GenerateTestsRequest,
    GenerateTestsResponse,
    AnalysisResponse,
    ErrorResponse,
)

logger = logging.getLogger(__name__)

router = APIRouter()

# Dependency to get validation engine
_engine: ValidationEngine | None = None


async def get_engine() -> ValidationEngine:
    """Get or create the validation engine instance."""
    global _engine
    if _engine is None:
        _engine = ValidationEngine()
        await _engine.initialize()
    return _engine


# ============================================================================
# Health & Status
# ============================================================================

@router.get(
    "/health",
    response_model=HealthResponse,
    tags=["Health"],
    summary="Health check",
)
async def health_check():
    """Check API health status."""
    return HealthResponse(
        status="healthy",
        version=__version__,
        timestamp=datetime.utcnow(),
    )


@router.get(
    "/connections",
    response_model=ConnectionsResponse,
    tags=["Health"],
    summary="Test database connections",
)
async def test_connections(engine: ValidationEngine = Depends(get_engine)):
    """Test connections to source and target databases."""
    results = await engine.test_connections()

    return ConnectionsResponse(
        source=ConnectionStatus(
            connected=results["source"][0],
            message=results["source"][1],
        ),
        target=ConnectionStatus(
            connected=results["target"][0],
            message=results["target"][1],
        ),
    )


# ============================================================================
# Schema
# ============================================================================

@router.get(
    "/schema/{database}",
    response_model=SchemaResponse,
    tags=["Schema"],
    summary="Get database schema",
)
async def get_schema(
    database: str,
    refresh: bool = Query(False, description="Force refresh schema cache"),
    engine: ValidationEngine = Depends(get_engine),
):
    """
    Get schema information for a database.
    
    - **database**: Either 'source' or 'target'
    - **refresh**: If true, bypasses cache and fetches fresh schema
    """
    if database not in ("source", "target"):
        raise HTTPException(400, "Database must be 'source' or 'target'")

    if database == "source":
        schema = await engine.get_source_schema(refresh=refresh)
    else:
        schema = await engine.get_target_schema(refresh=refresh)

    return SchemaResponse(
        database=database,
        tables=schema.get("tables", {}),
        views=schema.get("views", []),
        schemas=schema.get("schemas", []),
    )


@router.get(
    "/schema/compare",
    response_model=SchemaComparisonResponse,
    tags=["Schema"],
    summary="Compare source and target schemas",
)
async def compare_schemas(engine: ValidationEngine = Depends(get_engine)):
    """Compare schemas between source and target databases."""
    comparison = await engine.get_schema_comparison()
    return SchemaComparisonResponse(**comparison)


# ============================================================================
# Validation Runs
# ============================================================================

@router.post(
    "/validations/analyze",
    response_model=AnalysisResponse,
    tags=["Validation"],
    summary="Analyze business rules",
)
async def analyze_rules(
    request: BusinessRulesRequest,
    engine: ValidationEngine = Depends(get_engine),
):
    """
    Analyze business rules and generate test cases without executing them.
    
    Use this to preview what tests will be generated before creating a validation run.
    """
    analysis = await engine.analyze_business_rules(request.rules)
    return AnalysisResponse(**analysis)


@router.post(
    "/validations",
    response_model=ValidationRunResponse,
    tags=["Validation"],
    summary="Create a new validation run",
    status_code=201,
)
async def create_validation(
    request: CreateValidationRequest,
    engine: ValidationEngine = Depends(get_engine),
):
    """
    Create a new validation run from business rules.
    
    This analyzes the rules and creates test cases, but does not execute them.
    Use the execute endpoint to run the tests.
    """
    run = await engine.create_validation_run(
        name=request.name,
        description=request.description,
        business_rules=request.business_rules,
    )

    return _validation_run_to_response(run)


@router.post(
    "/validations/{run_id}/execute",
    response_model=ValidationRunResponse,
    tags=["Validation"],
    summary="Execute a validation run",
)
async def execute_validation(
    run_id: str,
    request: ExecuteValidationRequest = ExecuteValidationRequest(),
    engine: ValidationEngine = Depends(get_engine),
):
    """
    Execute all test cases in a validation run.
    
    Returns the completed validation run with all test results.
    """
    try:
        run = await engine.execute_validation_run(
            run_id=run_id,
            parallel_tests=request.parallel_tests,
        )
        return _validation_run_to_response(run)
    except ValueError as e:
        raise HTTPException(404, str(e))


@router.post(
    "/validations/quick",
    response_model=ValidationRunResponse,
    tags=["Validation"],
    summary="Quick validation (create + execute)",
)
async def quick_validation(
    request: QuickValidationRequest,
    engine: ValidationEngine = Depends(get_engine),
):
    """
    Create and execute a validation run in a single request.
    
    Convenient for quick validations without needing separate create/execute calls.
    """
    run = await engine.run_quick_validation(
        name=request.name,
        business_rules=request.business_rules,
    )
    return _validation_run_to_response(run)


@router.get(
    "/validations",
    response_model=ValidationRunListResponse,
    tags=["Validation"],
    summary="List validation runs",
)
async def list_validations(
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
    status: ValidationStatus | None = Query(None),
    engine: ValidationEngine = Depends(get_engine),
):
    """
    List validation runs with pagination and optional status filter.
    """
    runs = engine.list_validation_runs(limit=limit, offset=offset, status=status)
    total = len(runs)  # In production, get actual total from DB

    return ValidationRunListResponse(
        runs=[_validation_run_to_response(r) for r in runs],
        total=total,
        limit=limit,
        offset=offset,
    )


@router.get(
    "/validations/stats",
    response_model=ValidationStatisticsResponse,
    tags=["Validation"],
    summary="Get validation statistics",
)
async def get_statistics(engine: ValidationEngine = Depends(get_engine)):
    """Get overall validation statistics."""
    stats = engine.get_statistics()

    total_tests = stats.get("total_tests", 0)
    passed_tests = stats.get("passed_tests", 0)
    pass_rate = (passed_tests / total_tests * 100) if total_tests > 0 else 0

    return ValidationStatisticsResponse(
        **stats,
        pass_rate=round(pass_rate, 2),
    )


@router.get(
    "/validations/{run_id}",
    response_model=ValidationRunResponse,
    tags=["Validation"],
    summary="Get a validation run",
)
async def get_validation(
    run_id: str,
    engine: ValidationEngine = Depends(get_engine),
):
    """Get details of a specific validation run."""
    run = engine.get_validation_run(run_id)
    if not run:
        raise HTTPException(404, f"Validation run not found: {run_id}")
    return _validation_run_to_response(run)


@router.delete(
    "/validations/{run_id}",
    tags=["Validation"],
    summary="Delete a validation run",
    status_code=204,
)
async def delete_validation(
    run_id: str,
    engine: ValidationEngine = Depends(get_engine),
):
    """Delete a validation run and all associated test results."""
    deleted = engine.delete_validation_run(run_id)
    if not deleted:
        raise HTTPException(404, f"Validation run not found: {run_id}")
    return None


# ============================================================================
# Query Execution
# ============================================================================

@router.post(
    "/query/natural",
    response_model=NaturalLanguageQueryResponse,
    tags=["Query"],
    summary="Execute natural language query",
)
async def natural_language_query(
    request: NaturalLanguageQueryRequest,
    engine: ValidationEngine = Depends(get_engine),
):
    """
    Convert a natural language query to SQL and execute it.
    
    The AI will interpret your question and generate appropriate SQL queries.
    """
    result = await engine.run_natural_language_query(request.query)
    return NaturalLanguageQueryResponse(**result)


@router.post(
    "/query/execute",
    response_model=QueryResultResponse,
    tags=["Query"],
    summary="Execute SQL query",
)
async def execute_query(
    request: QueryExecutionRequest,
    engine: ValidationEngine = Depends(get_engine),
):
    """
    Execute a SQL query directly on the specified database.
    
    Only read-only queries are allowed.
    """
    # Safety check - ensure query is read-only
    query_upper = request.query.upper().strip()
    forbidden = ["INSERT", "UPDATE", "DELETE", "DROP", "TRUNCATE", "ALTER", "CREATE"]
    for keyword in forbidden:
        if keyword in query_upper:
            raise HTTPException(400, f"Only read-only queries allowed. Found: {keyword}")

    # Add limit if not present
    query = request.query
    if "LIMIT" not in query_upper:
        query = f"{request.query} LIMIT {request.limit}"

    result = await engine.executor.execute_query(query, request.database)

    return QueryResultResponse(
        query=result.query,
        database=result.database,
        success=result.success,
        rows=result.rows,
        row_count=result.row_count,
        columns=result.columns,
        execution_time_ms=result.execution_time_ms,
        error=result.error,
        timestamp=result.timestamp,
    )


# ============================================================================
# Test Generation
# ============================================================================

@router.post(
    "/tests/generate",
    response_model=GenerateTestsResponse,
    tags=["Tests"],
    summary="Generate comprehensive tests",
)
async def generate_tests(
    request: GenerateTestsRequest = GenerateTestsRequest(),
    engine: ValidationEngine = Depends(get_engine),
):
    """
    Generate a comprehensive set of validation tests based on database schemas.
    
    This uses AI to analyze the schemas and generate appropriate tests.
    """
    result = await engine.generate_standard_tests(
        focus_areas=request.focus_areas,
    )
    return GenerateTestsResponse(**result)


# ============================================================================
# Helper Functions
# ============================================================================

def _validation_run_to_response(run) -> ValidationRunResponse:
    """Convert ValidationRun model to response schema."""
    test_cases = [
        TestCaseResponse(
            id=tc.id,
            name=tc.name,
            description=tc.description,
            business_rule=tc.business_rule,
            source_query=tc.source_query,
            target_query=tc.target_query,
            validation_type=tc.validation_type,
            status=tc.status,
            actual_result=tc.actual_result,
            error_message=tc.error_message,
            execution_time_ms=tc.execution_time_ms,
            executed_at=tc.executed_at,
            evidence=tc.evidence,
        )
        for tc in run.test_cases
    ]

    return ValidationRunResponse(
        id=run.id,
        name=run.name,
        description=run.description,
        business_rules=run.business_rules,
        status=run.status,
        total_tests=run.total_tests,
        passed_tests=run.passed_tests,
        failed_tests=run.failed_tests,
        error_tests=run.error_tests,
        skipped_tests=run.skipped_tests,
        started_at=run.started_at,
        completed_at=run.completed_at,
        execution_time_ms=run.execution_time_ms,
        test_cases=test_cases,
    )
