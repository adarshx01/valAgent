"""
CLI interface for ETL Validation Agent.

Provides command-line interface for running validations.
"""

import asyncio
import sys
from pathlib import Path
from typing import Optional
import typer
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich.progress import Progress, SpinnerColumn, TextColumn
from rich.markdown import Markdown
from rich.syntax import Syntax

from .agents.validation_agent import validation_agent
from .utils.logger import setup_logging, get_logger

logger = get_logger(__name__)
console = Console()
app = typer.Typer(
    name="etl-validator",
    help="ETL Validation Agent - AI-powered data pipeline validation",
    add_completion=False,
)


@app.command("validate")
def validate(
    rules_file: Optional[Path] = typer.Option(
        None,
        "--rules", "-r",
        help="Path to file containing business rules",
    ),
    rules: Optional[str] = typer.Option(
        None,
        "--text", "-t",
        help="Business rules as text",
    ),
    output: Optional[Path] = typer.Option(
        None,
        "--output", "-o",
        help="Output file for report (markdown)",
    ),
    verbose: bool = typer.Option(
        False,
        "--verbose", "-v",
        help="Enable verbose output",
    ),
):
    """
    Run ETL validation with business rules.
    
    Provide rules either via file (--rules) or text (--text).
    """
    setup_logging(level="DEBUG" if verbose else "INFO")

    # Get rules
    if rules_file:
        if not rules_file.exists():
            console.print(f"[red]Error: File not found: {rules_file}[/red]")
            raise typer.Exit(1)
        business_rules = rules_file.read_text()
    elif rules:
        business_rules = rules
    else:
        console.print("[yellow]Enter business rules (Ctrl+D when done):[/yellow]")
        business_rules = sys.stdin.read()

    if not business_rules.strip():
        console.print("[red]Error: No business rules provided[/red]")
        raise typer.Exit(1)

    console.print(Panel.fit(
        "[bold blue]ETL Validation Agent[/bold blue]\n"
        "Starting validation...",
        border_style="blue",
    ))

    async def run_validation():
        try:
            with Progress(
                SpinnerColumn(),
                TextColumn("[progress.description]{task.description}"),
                console=console,
            ) as progress:
                task = progress.add_task("Initializing...", total=None)

                await validation_agent.initialize()
                progress.update(task, description="Running validation...")

                report = await validation_agent.validate(
                    business_rules=business_rules,
                    validation_name="CLI Validation",
                )

                progress.update(task, description="Complete!")

            return report

        except Exception as e:
            console.print(f"[red]Validation failed: {e}[/red]")
            raise typer.Exit(1)
        finally:
            await validation_agent.close()

    report = asyncio.run(run_validation())

    # Display results
    console.print()
    console.print(Panel.fit(
        f"[bold]Validation Complete[/bold]\n\n"
        f"Status: [{'green' if report.overall_status.value == 'passed' else 'red'}]"
        f"{report.overall_status.value.upper()}[/]\n"
        f"Passed: {report.execution_summary.passed}/{report.execution_summary.total_tests}\n"
        f"Pass Rate: {report.execution_summary.pass_rate:.1f}%\n"
        f"Duration: {report.execution_summary.total_duration_ms:.0f}ms",
        title="Results",
        border_style="green" if report.overall_status.value == "passed" else "red",
    ))

    # Show test results table
    table = Table(title="Test Results")
    table.add_column("Test", style="cyan")
    table.add_column("Status", justify="center")
    table.add_column("Duration", justify="right")
    table.add_column("Message")

    for result in report.test_results:
        status_color = "green" if result.status.value == "passed" else "red"
        table.add_row(
            result.test_case_name[:40],
            f"[{status_color}]{result.status.value.upper()}[/]",
            f"{result.duration_ms:.0f}ms",
            result.message[:50],
        )

    console.print(table)

    # Show AI analysis
    if report.ai_analysis:
        console.print()
        console.print(Panel(
            Markdown(report.ai_analysis),
            title="AI Analysis",
            border_style="blue",
        ))

    # Show recommendations
    if report.ai_recommendations:
        console.print()
        console.print("[bold]Recommendations:[/bold]")
        for rec in report.ai_recommendations:
            console.print(f"  • {rec}")

    # Save report if output specified
    if output:
        output.write_text(report.to_markdown())
        console.print(f"\n[green]Report saved to: {output}[/green]")

    # Exit with appropriate code
    if report.overall_status.value != "passed":
        raise typer.Exit(1)


@app.command("query")
def query(
    sql: str = typer.Argument(..., help="SQL query to execute"),
    database: str = typer.Option(
        "target",
        "--db", "-d",
        help="Database to query (source/target)",
    ),
):
    """Execute an ad-hoc SQL query."""
    setup_logging(level="WARNING")

    async def run_query():
        try:
            await validation_agent.initialize()
            result = await validation_agent.execute_query(sql, database)
            return result
        finally:
            await validation_agent.close()

    result = asyncio.run(run_query())

    if result.get("success"):
        console.print(f"[green]Query executed successfully[/green]")
        console.print(f"Rows returned: {result.get('row_count', 0)}")
        console.print(f"Execution time: {result.get('proof', {}).get('execution_time_ms', 0):.2f}ms")

        if result.get("data"):
            table = Table()
            data = result["data"]

            # Add columns
            if data:
                for col in data[0].keys():
                    table.add_column(col)

                # Add rows (limit to 20)
                for row in data[:20]:
                    table.add_row(*[str(v) for v in row.values()])

            console.print(table)

            if len(data) > 20:
                console.print(f"[dim]... and {len(data) - 20} more rows[/dim]")
    else:
        console.print(f"[red]Query failed: {result.get('error')}[/red]")
        raise typer.Exit(1)


@app.command("schema")
def schema(
    database: str = typer.Argument(
        "target",
        help="Database to show schema for (source/target/compare)",
    ),
):
    """Show database schema information."""
    setup_logging(level="WARNING")

    async def get_schema():
        try:
            await validation_agent.initialize()

            if database == "compare":
                return await validation_agent.compare_databases()
            else:
                return await validation_agent.get_database_info()
        finally:
            await validation_agent.close()

    info = asyncio.run(get_schema())

    if database == "compare":
        console.print(Panel.fit(
            f"[bold]Schema Comparison[/bold]\n\n"
            f"Source Tables: {info.get('summary', {}).get('total_source_tables', 0)}\n"
            f"Target Tables: {info.get('summary', {}).get('total_target_tables', 0)}\n"
            f"Matching: {info.get('summary', {}).get('matching_tables', 0)}\n"
            f"Differences: {len(info.get('differences', []))}",
            border_style="blue",
        ))
    else:
        db_info = info.get(database, {})
        console.print(Panel.fit(
            f"[bold]{database.capitalize()} Database[/bold]\n\n"
            f"Tables: {db_info.get('tables', 0)}",
            border_style="blue",
        ))

        if "schema" in db_info:
            table = Table(title="Tables")
            table.add_column("Table")
            table.add_column("Columns", justify="right")
            table.add_column("Rows", justify="right")

            for table_name, table_info in list(db_info["schema"].items())[:20]:
                table.add_row(
                    table_name,
                    str(table_info.get("columns", 0)),
                    str(table_info.get("row_count", 0)),
                )

            console.print(table)


@app.command("serve")
def serve(
    host: str = typer.Option("0.0.0.0", "--host", "-h", help="Host to bind to"),
    port: int = typer.Option(8000, "--port", "-p", help="Port to bind to"),
    reload: bool = typer.Option(False, "--reload", help="Enable auto-reload"),
):
    """Start the API server."""
    import uvicorn

    console.print(Panel.fit(
        f"[bold blue]ETL Validation Agent API Server[/bold blue]\n\n"
        f"Starting server at http://{host}:{port}\n"
        f"API Docs: http://{host}:{port}/docs",
        border_style="blue",
    ))

    uvicorn.run(
        "src.etl_validator.api.app:app",
        host=host,
        port=port,
        reload=reload,
    )


@app.command("generate-sql")
def generate_sql(
    description: str = typer.Argument(..., help="Natural language description"),
    database: str = typer.Option("target", "--db", "-d", help="Target database"),
):
    """Generate SQL from natural language description."""
    setup_logging(level="WARNING")

    async def run_generate():
        try:
            await validation_agent.initialize()
            return await validation_agent.generate_sql(description, database)
        finally:
            await validation_agent.close()

    sql = asyncio.run(run_generate())

    console.print("[bold]Generated SQL:[/bold]")
    console.print(Syntax(sql, "sql", theme="monokai", line_numbers=True))


def main():
    """Main entry point for CLI."""
    app()


if __name__ == "__main__":
    main()
