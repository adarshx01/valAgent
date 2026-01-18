"""
Report generator for creating validation reports in various formats.
"""

import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Any
import uuid

from jinja2 import Environment, BaseLoader

from valagent.config import get_settings
from valagent.database.models import ValidationRun, ValidationStatus
from valagent.reports.templates import ReportTemplates

logger = logging.getLogger(__name__)


class ReportGenerator:
    """
    Generates validation reports in multiple formats.
    Supports HTML, Markdown, JSON, and PDF output.
    """

    def __init__(self, output_dir: str | None = None):
        settings = get_settings()
        self.output_dir = Path(output_dir or settings.app.reports_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.templates = ReportTemplates()
        self.jinja_env = Environment(loader=BaseLoader())

    def _render_template(self, template_str: str, context: dict[str, Any]) -> str:
        """Render a Jinja2 template with the given context."""
        template = self.jinja_env.from_string(template_str)
        return template.render(**context)

    def _build_context(self, run: ValidationRun) -> dict[str, Any]:
        """Build the template context from a validation run."""
        total = run.total_tests
        passed = run.passed_tests
        pass_rate = (passed / total * 100) if total > 0 else 0

        return {
            "run": run,
            "pass_rate": round(pass_rate, 1),
            "generated_at": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC"),
            "current_year": datetime.utcnow().year,
        }

    def generate_html_report(
        self,
        run: ValidationRun,
        output_path: str | None = None,
    ) -> str:
        """
        Generate an HTML report for a validation run.
        
        Args:
            run: The validation run to report on
            output_path: Optional path for the output file
            
        Returns:
            Path to the generated report file
        """
        context = self._build_context(run)
        html_content = self._render_template(self.templates.HTML_REPORT, context)

        if output_path is None:
            filename = f"validation_report_{run.id[:8]}_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.html"
            output_path = str(self.output_dir / filename)

        with open(output_path, "w", encoding="utf-8") as f:
            f.write(html_content)

        logger.info(f"Generated HTML report: {output_path}")
        return output_path

    def generate_markdown_report(
        self,
        run: ValidationRun,
        output_path: str | None = None,
    ) -> str:
        """
        Generate a Markdown report for a validation run.
        
        Args:
            run: The validation run to report on
            output_path: Optional path for the output file
            
        Returns:
            Path to the generated report file
        """
        context = self._build_context(run)
        md_content = self._render_template(self.templates.MARKDOWN_REPORT, context)

        if output_path is None:
            filename = f"validation_report_{run.id[:8]}_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.md"
            output_path = str(self.output_dir / filename)

        with open(output_path, "w", encoding="utf-8") as f:
            f.write(md_content)

        logger.info(f"Generated Markdown report: {output_path}")
        return output_path

    def generate_json_report(
        self,
        run: ValidationRun,
        output_path: str | None = None,
        pretty: bool = True,
    ) -> str:
        """
        Generate a JSON report for a validation run.
        
        Args:
            run: The validation run to report on
            output_path: Optional path for the output file
            pretty: Whether to format the JSON with indentation
            
        Returns:
            Path to the generated report file
        """
        total = run.total_tests
        passed = run.passed_tests
        pass_rate = (passed / total * 100) if total > 0 else 0

        report_data = {
            "report_id": str(uuid.uuid4()),
            "generated_at": datetime.utcnow().isoformat(),
            "version": "1.0",
            "validation_run": {
                "id": run.id,
                "name": run.name,
                "description": run.description,
                "status": run.status.value,
                "business_rules": run.business_rules,
                "started_at": run.started_at.isoformat() if run.started_at else None,
                "completed_at": run.completed_at.isoformat() if run.completed_at else None,
                "execution_time_ms": run.execution_time_ms,
            },
            "summary": {
                "total_tests": run.total_tests,
                "passed_tests": run.passed_tests,
                "failed_tests": run.failed_tests,
                "error_tests": run.error_tests,
                "skipped_tests": run.skipped_tests,
                "pass_rate_percent": round(pass_rate, 2),
            },
            "test_results": [
                {
                    "id": tc.id,
                    "name": tc.name,
                    "description": tc.description,
                    "business_rule": tc.business_rule,
                    "validation_type": tc.validation_type,
                    "status": tc.status.value,
                    "source_query": tc.source_query,
                    "target_query": tc.target_query,
                    "expected_result": tc.expected_result,
                    "actual_result": tc.actual_result,
                    "error_message": tc.error_message,
                    "execution_time_ms": tc.execution_time_ms,
                    "executed_at": tc.executed_at.isoformat() if tc.executed_at else None,
                    "evidence": tc.evidence,
                }
                for tc in run.test_cases
            ],
        }

        if output_path is None:
            filename = f"validation_report_{run.id[:8]}_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.json"
            output_path = str(self.output_dir / filename)

        with open(output_path, "w", encoding="utf-8") as f:
            if pretty:
                json.dump(report_data, f, indent=2, default=str)
            else:
                json.dump(report_data, f, default=str)

        logger.info(f"Generated JSON report: {output_path}")
        return output_path

    def generate_summary_report(
        self,
        runs: list[ValidationRun],
        output_path: str | None = None,
    ) -> str:
        """
        Generate a summary report for multiple validation runs.
        Useful for executive summaries and trend analysis.
        """
        total_runs = len(runs)
        passed_runs = sum(1 for r in runs if r.status == ValidationStatus.PASSED)
        failed_runs = sum(1 for r in runs if r.status == ValidationStatus.FAILED)

        total_tests = sum(r.total_tests for r in runs)
        passed_tests = sum(r.passed_tests for r in runs)
        failed_tests = sum(r.failed_tests for r in runs)
        error_tests = sum(r.error_tests for r in runs)

        summary_data = {
            "report_type": "summary",
            "generated_at": datetime.utcnow().isoformat(),
            "period": {
                "start": min(r.started_at for r in runs if r.started_at).isoformat() if runs else None,
                "end": max(r.completed_at for r in runs if r.completed_at).isoformat() if runs else None,
            },
            "runs_summary": {
                "total_runs": total_runs,
                "passed_runs": passed_runs,
                "failed_runs": failed_runs,
                "run_pass_rate_percent": round((passed_runs / total_runs * 100) if total_runs > 0 else 0, 2),
            },
            "tests_summary": {
                "total_tests": total_tests,
                "passed_tests": passed_tests,
                "failed_tests": failed_tests,
                "error_tests": error_tests,
                "test_pass_rate_percent": round((passed_tests / total_tests * 100) if total_tests > 0 else 0, 2),
            },
            "runs": [
                {
                    "id": r.id,
                    "name": r.name,
                    "status": r.status.value,
                    "pass_rate": round((r.passed_tests / r.total_tests * 100) if r.total_tests > 0 else 0, 2),
                    "completed_at": r.completed_at.isoformat() if r.completed_at else None,
                }
                for r in sorted(runs, key=lambda x: x.completed_at or datetime.min, reverse=True)
            ],
        }

        if output_path is None:
            filename = f"summary_report_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.json"
            output_path = str(self.output_dir / filename)

        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(summary_data, f, indent=2, default=str)

        logger.info(f"Generated summary report: {output_path}")
        return output_path

    def generate_all_formats(
        self,
        run: ValidationRun,
        base_name: str | None = None,
    ) -> dict[str, str]:
        """
        Generate reports in all available formats.
        
        Returns:
            Dictionary mapping format names to file paths
        """
        if base_name is None:
            base_name = f"validation_report_{run.id[:8]}_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}"

        reports = {}

        # HTML
        html_path = str(self.output_dir / f"{base_name}.html")
        reports["html"] = self.generate_html_report(run, html_path)

        # Markdown
        md_path = str(self.output_dir / f"{base_name}.md")
        reports["markdown"] = self.generate_markdown_report(run, md_path)

        # JSON
        json_path = str(self.output_dir / f"{base_name}.json")
        reports["json"] = self.generate_json_report(run, json_path)

        logger.info(f"Generated reports in all formats: {list(reports.keys())}")
        return reports

    def get_report_content(
        self,
        run: ValidationRun,
        format: str = "html",
    ) -> str:
        """
        Get report content as a string without saving to file.
        
        Args:
            run: The validation run to report on
            format: Output format (html, markdown, json)
            
        Returns:
            Report content as a string
        """
        context = self._build_context(run)

        if format == "html":
            return self._render_template(self.templates.HTML_REPORT, context)
        elif format == "markdown":
            return self._render_template(self.templates.MARKDOWN_REPORT, context)
        elif format == "json":
            total = run.total_tests
            passed = run.passed_tests
            pass_rate = (passed / total * 100) if total > 0 else 0

            report_data = {
                "validation_run": run.model_dump(),
                "summary": {
                    "pass_rate_percent": round(pass_rate, 2),
                },
                "generated_at": datetime.utcnow().isoformat(),
            }
            return json.dumps(report_data, indent=2, default=str)
        else:
            raise ValueError(f"Unsupported format: {format}")

    def cleanup_old_reports(self, max_age_days: int | None = None) -> int:
        """
        Remove reports older than the specified age.
        
        Returns:
            Number of reports deleted
        """
        settings = get_settings()
        max_age_days = max_age_days or settings.app.max_report_age_days

        deleted = 0
        cutoff = datetime.utcnow().timestamp() - (max_age_days * 24 * 60 * 60)

        for report_file in self.output_dir.glob("validation_report_*"):
            if report_file.stat().st_mtime < cutoff:
                report_file.unlink()
                deleted += 1
                logger.debug(f"Deleted old report: {report_file}")

        if deleted > 0:
            logger.info(f"Cleaned up {deleted} old reports")

        return deleted
