"""
ETL Validation Agent - Main Entry Point.

This is the main entry point for running the ETL Validation Agent
as a FastAPI application or CLI tool.
"""

import asyncio
import sys
from pathlib import Path

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent))

from src.etl_validator.utils.logger import setup_logging, get_logger
from src.etl_validator.core.config import settings

logger = get_logger(__name__)


def run_server():
    """Run the FastAPI server."""
    import uvicorn
    from src.etl_validator.api.app import app

    setup_logging(level=settings.log_level)
    logger.info(f"Starting {settings.app_name} v{settings.app_version}")

    uvicorn.run(
        app,
        host="0.0.0.0",
        port=8000,
        log_level=settings.log_level.lower(),
    )


def run_cli():
    """Run the CLI interface."""
    from src.etl_validator.cli import main as cli_main
    cli_main()


async def run_validation_example():
    """Run a sample validation (for testing)."""
    from src.etl_validator.agents.validation_agent import validation_agent

    setup_logging(level="INFO")
    logger.info("Running example validation...")

    try:
        await validation_agent.initialize()

        # Example business rules
        business_rules = """
        1. All records from the source customers table should exist in the target customers table.
        2. Email addresses should be stored in lowercase format in the target database.
        3. The total count of orders should match between source and target.
        4. Customer names should not contain any NULL values in the target.
        5. All dates should be in UTC timezone in the target database.
        """

        report = await validation_agent.validate(
            business_rules=business_rules,
            validation_name="Example Validation",
        )

        print("\n" + "=" * 60)
        print("VALIDATION REPORT")
        print("=" * 60)
        print(report.to_markdown())
        print("=" * 60)

        return report

    finally:
        await validation_agent.close()


def main():
    """Main entry point."""
    if len(sys.argv) > 1:
        command = sys.argv[1]

        if command == "serve":
            run_server()
        elif command == "cli":
            # Remove 'cli' from args and run CLI
            sys.argv = [sys.argv[0]] + sys.argv[2:]
            run_cli()
        elif command == "example":
            asyncio.run(run_validation_example())
        else:
            # Pass to CLI
            run_cli()
    else:
        # Default: run server
        run_server()


if __name__ == "__main__":
    main()
