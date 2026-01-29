"""
API Routes for ETL Validation Agent.

Defines all REST API endpoints for the validation service.
"""

from typing import Any
from fastapi import APIRouter, HTTPException, BackgroundTasks
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field
import json

from ..agents.validation_agent import validation_agent
from ..services.validation_orchestrator import orchestrator
from ..core.database import db_manager
from ..utils.logger import get_logger
from ..utils.helpers import generate_uuid

logger = get_logger(__name__)

router = APIRouter(tags=["Validation"])


# Request/Response Models
class ValidationRequest(BaseModel):
    """Request model for validation."""

    business_rules: str = Field(
        ...,
        description="Business rules in natural language",
        min_length=10,
        examples=[
            "1. All customer records from source should exist in target\n"
            "2. Email addresses should be lowercase in target\n"
            "3. Total order amounts should match between source and target"
        ],
    )
    validation_name: str | None = Field(
        None,
        description="Optional name for this validation run",
        examples=["Monthly Customer Data Validation"],
    )


class QueryRequest(BaseModel):
    """Request model for ad-hoc query execution."""

    sql: str = Field(
        ...,
        description="SQL query to execute",
        examples=["SELECT COUNT(*) FROM public.customers"],
    )
    database: str = Field(
        default="target",
        description="Database to run query on",
        pattern="^(source|target)$",
    )


class SQLGenerationRequest(BaseModel):
    """Request model for SQL generation."""

    description: str = Field(
        ...,
        description="Natural language description of the query",
        examples=["Count all active customers created in the last 30 days"],
    )
    database: str = Field(
        default="target",
        description="Target database for context",
        pattern="^(source|target)$",
    )


class QuickValidationRequest(BaseModel):
    """Request model for quick single-rule validation."""

    rule: str = Field(
        ...,
        description="Single business rule to validate",
        examples=["Row count of orders table should match between source and target"],
    )


# Endpoints
@router.post("/validate", summary="Run Full Validation")
async def run_validation(request: ValidationRequest) -> dict[str, Any]:
    """
    Run complete ETL validation with business rules.
    
    This endpoint:
    1. Parses natural language business rules
    2. Extracts source and target database schemas
    3. Generates comprehensive test cases
    4. Executes all tests with parallel processing
    5. Analyzes results and generates detailed report
    
    Returns a complete validation report with:
    - Execution summary (pass/fail counts, duration)
    - Individual test results with proofs
    - AI-generated analysis and recommendations
    """
    try:
        report = await validation_agent.validate(
            business_rules=request.business_rules,
            validation_name=request.validation_name,
        )

        return {
            "success": True,
            "report": report.to_json_summary(),
            "markdown_report": report.to_markdown(),
            "test_results": [r.to_summary() for r in report.test_results],
        }

    except Exception as e:
        logger.error(f"Validation failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/validate/stream", summary="Run Validation with Streaming")
async def run_validation_streaming(request: ValidationRequest):
    """
    Run validation with streaming progress updates.
    
    Returns Server-Sent Events (SSE) with progress updates:
    - Schema extraction progress
    - Rule parsing status
    - Test case generation progress
    - Execution progress
    - Final report
    """

    async def generate():
        async for update in validation_agent.validate_streaming(
            business_rules=request.business_rules,
            validation_name=request.validation_name,
        ):
            yield f"data: {json.dumps(update)}\n\n"

    return StreamingResponse(
        generate(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
        },
    )


@router.post("/validate/quick", summary="Quick Single-Rule Validation")
async def quick_validation(request: QuickValidationRequest) -> dict[str, Any]:
    """
    Quick validation of a single business rule.
    
    Faster than full validation, suitable for testing individual rules.
    """
    try:
        result = await validation_agent.quick_validate(request.rule)
        return result

    except Exception as e:
        logger.error(f"Quick validation failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/query/execute", summary="Execute Ad-hoc Query")
async def execute_query(request: QueryRequest) -> dict[str, Any]:
    """
    Execute an ad-hoc SQL query on source or target database.
    
    Returns query results with execution proof including:
    - Execution time
    - Row count
    - Sample data
    """
    try:
        result = await validation_agent.execute_query(
            query=request.sql,
            database=request.database,
        )
        return result

    except Exception as e:
        logger.error(f"Query execution failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/query/generate", summary="Generate SQL from Description")
async def generate_sql(request: SQLGenerationRequest) -> dict[str, Any]:
    """
    Generate SQL query from natural language description.
    
    Uses AI to convert your description into a valid PostgreSQL query.
    """
    try:
        sql = await validation_agent.generate_sql(
            description=request.description,
            database=request.database,
        )
        return {
            "success": True,
            "sql": sql,
            "database": request.database,
        }

    except Exception as e:
        logger.error(f"SQL generation failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/schema/source", summary="Get Source Database Schema")
async def get_source_schema() -> dict[str, Any]:
    """
    Get source database schema information.
    
    Returns tables, columns, relationships, and row counts.
    """
    try:
        info = await orchestrator.get_schema_info("source")
        return {
            "success": True,
            "schema": info,
        }

    except Exception as e:
        logger.error(f"Failed to get source schema: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/schema/target", summary="Get Target Database Schema")
async def get_target_schema() -> dict[str, Any]:
    """
    Get target database schema information.
    
    Returns tables, columns, relationships, and row counts.
    """
    try:
        info = await orchestrator.get_schema_info("target")
        return {
            "success": True,
            "schema": info,
        }

    except Exception as e:
        logger.error(f"Failed to get target schema: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/schema/compare", summary="Compare Source and Target Schemas")
async def compare_schemas() -> dict[str, Any]:
    """
    Compare source and target database schemas.
    
    Identifies:
    - Tables missing in source or target
    - Column differences (type, nullable)
    - Matching tables
    """
    try:
        comparison = await orchestrator.compare_schemas()
        return {
            "success": True,
            "comparison": comparison,
        }

    except Exception as e:
        logger.error(f"Schema comparison failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/databases/info", summary="Get Database Connection Info")
async def get_database_info() -> dict[str, Any]:
    """
    Get information about connected databases.
    
    Returns connection status and basic statistics.
    """
    try:
        info = await validation_agent.get_database_info()
        return {
            "success": True,
            "databases": info,
        }

    except Exception as e:
        logger.error(f"Failed to get database info: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/status", summary="Get Agent Status")
async def get_agent_status() -> dict[str, Any]:
    """
    Get current status of the validation agent.
    """
    return {
        "status": "ready",
        "agent_initialized": validation_agent._initialized,
        "session_id": validation_agent._current_session_id,
    }
