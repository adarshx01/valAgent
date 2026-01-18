"""
FastAPI application factory and configuration.
"""

import logging
from contextlib import asynccontextmanager
from datetime import datetime
from pathlib import Path

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, FileResponse
from fastapi.staticfiles import StaticFiles
from fastapi.exceptions import RequestValidationError

from valagent import __version__
from valagent.config import get_settings
from valagent.api.routes import router, get_engine
from valagent.api.schemas import ErrorResponse, ValidationErrorResponse

# Get the static files directory
STATIC_DIR = Path(__file__).parent.parent / "static"

logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan manager."""
    # Startup
    logger.info("Starting ValAgent API...")
    settings = get_settings()
    logger.info(f"Environment: {settings.app.app_env}")

    # Initialize engine
    engine = await get_engine()
    logger.info("Validation engine initialized")

    yield

    # Shutdown
    logger.info("Shutting down ValAgent API...")
    if engine:
        await engine.close()


def create_app() -> FastAPI:
    """Create and configure the FastAPI application."""
    settings = get_settings()

    app = FastAPI(
        title="ValAgent - Data Validation Agent",
        description="""
## 🎯 Enterprise ETL Validation Agent

ValAgent is an intelligent data validation platform that automates ETL (Extract, Transform, Load) 
pipeline testing using natural language business rules.

### Key Features

- **🗣️ Natural Language Rules**: Express validation rules in plain English
- **🤖 AI-Powered SQL Generation**: Automatically generates SQL queries from business rules
- **📊 Comprehensive Testing**: Covers row counts, data accuracy, referential integrity, and more
- **📈 Detailed Reporting**: Get proof of execution with sample data and statistics
- **🔄 Schema Comparison**: Compare source and target database structures

### How It Works

1. **Define Rules**: Input your business rules in natural language
2. **Generate Tests**: AI analyzes rules and creates comprehensive test cases
3. **Execute Validation**: Tests run against source and target databases
4. **Review Results**: Get detailed pass/fail reports with evidence

### Use Cases

- ETL pipeline validation
- Data migration verification
- Data quality monitoring
- Compliance testing
        """,
        version=__version__,
        lifespan=lifespan,
        docs_url="/docs",
        redoc_url="/redoc",
        openapi_url="/openapi.json",
    )

    # Configure CORS
    app.add_middleware(
        CORSMiddleware,
        allow_origins=settings.app.cors_origins,
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    # Exception handlers
    @app.exception_handler(RequestValidationError)
    async def validation_exception_handler(
        request: Request,
        exc: RequestValidationError,
    ):
        """Handle validation errors with detailed response."""
        return JSONResponse(
            status_code=422,
            content=ValidationErrorResponse(
                error="Validation Error",
                detail=[
                    {
                        "loc": e.get("loc"),
                        "msg": e.get("msg"),
                        "type": e.get("type"),
                    }
                    for e in exc.errors()
                ],
            ).model_dump(),
        )

    @app.exception_handler(Exception)
    async def general_exception_handler(request: Request, exc: Exception):
        """Handle unexpected errors."""
        logger.error(f"Unexpected error: {exc}", exc_info=True)
        return JSONResponse(
            status_code=500,
            content=ErrorResponse(
                error="Internal Server Error",
                detail=str(exc) if settings.app.debug else None,
            ).model_dump(),
        )

    # Include routers
    app.include_router(router, prefix="/api/v1")

    # Mount static files
    if STATIC_DIR.exists():
        app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")

    # Root endpoint - serve the frontend
    @app.get("/", tags=["Root"], include_in_schema=False)
    async def root():
        """Serve the frontend application."""
        index_file = STATIC_DIR / "index.html"
        if index_file.exists():
            return FileResponse(str(index_file))
        return {
            "name": "ValAgent - Data Validation Agent",
            "version": __version__,
            "status": "running",
            "docs": "/docs",
            "api": "/api/v1",
            "timestamp": datetime.utcnow().isoformat(),
        }

    # API info endpoint
    @app.get("/api", tags=["Root"])
    async def api_info():
        """API information endpoint."""
        return {
            "name": "ValAgent - Data Validation Agent",
            "version": __version__,
            "status": "running",
            "docs": "/docs",
            "api": "/api/v1",
            "timestamp": datetime.utcnow().isoformat(),
        }

    return app


# Create app instance for running with uvicorn
app = create_app()
