"""
FastAPI Application Factory.

Creates and configures the FastAPI application with all routes and middleware.
"""

from contextlib import asynccontextmanager
from typing import AsyncGenerator
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from ..core.config import settings
from ..core.database import db_manager
from ..core.exceptions import ETLValidatorError
from ..services.llm_service import llm_service
from ..utils.logger import setup_logging, get_logger
from .routes import router

logger = get_logger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    """Application lifespan handler for startup and shutdown."""
    # Startup
    logger.info("Starting ETL Validation Agent...")
    setup_logging(level=settings.log_level)

    try:
        # Initialize database connections
        await db_manager.initialize()
        logger.info("Database connections established")

        # Initialize LLM service
        await llm_service.initialize()
        logger.info("LLM service initialized")

        yield

    finally:
        # Shutdown
        logger.info("Shutting down ETL Validation Agent...")
        await db_manager.close()
        logger.info("Cleanup complete")


def create_app() -> FastAPI:
    """
    Create and configure the FastAPI application.
    
    Returns:
        Configured FastAPI application instance
    """
    app = FastAPI(
        title=settings.app_name,
        description="""
# ETL Validation Agent API

An intelligent AI-powered agent for validating ETL (Extract, Transform & Load) jobs 
based on natural language business rules.

## Features

- **Natural Language Rules**: Define validation rules in plain English
- **Automated Testing**: Agent generates and executes comprehensive test cases
- **Parallel Processing**: Handles large datasets with PostgreSQL parallel queries
- **Detailed Reports**: Get pass/fail results with proof of execution
- **AI Analysis**: Intelligent analysis of results with recommendations

## Workflow

1. Submit business rules in natural language
2. Agent extracts source and target database schemas
3. AI generates appropriate SQL test cases
4. Tests execute in parallel for performance
5. Results are analyzed and reported with proofs
        """,
        version=settings.app_version,
        lifespan=lifespan,
        docs_url="/docs",
        redoc_url="/redoc",
    )

    # Add CORS middleware
    app.add_middleware(
        CORSMiddleware,
        allow_origins=settings.cors_origins_list,
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    # Add exception handlers
    @app.exception_handler(ETLValidatorError)
    async def etl_validator_exception_handler(request, exc: ETLValidatorError):
        return JSONResponse(
            status_code=400,
            content=exc.to_dict(),
        )

    @app.exception_handler(Exception)
    async def general_exception_handler(request, exc: Exception):
        logger.error(f"Unhandled exception: {exc}", exc_info=True)
        return JSONResponse(
            status_code=500,
            content={
                "error_code": "INTERNAL_ERROR",
                "message": "An internal error occurred",
                "details": {"error": str(exc)} if settings.debug else {},
            },
        )

    # Include routes
    app.include_router(router, prefix="/api/v1")

    # Health check at root
    @app.get("/health", tags=["Health"])
    async def health_check():
        """Health check endpoint."""
        return {
            "status": "healthy",
            "service": settings.app_name,
            "version": settings.app_version,
        }

    return app


# Create app instance
app = create_app()
