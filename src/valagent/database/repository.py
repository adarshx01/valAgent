"""
Repository for storing and managing validation runs and test results.
Uses SQLite for local persistence of validation history.
"""

import json
from datetime import datetime
from pathlib import Path
from typing import Any
import sqlite3
import logging

from valagent.database.models import ValidationRun, TestCase, ValidationStatus

logger = logging.getLogger(__name__)


class ValidationRepository:
    """
    Manages persistence of validation runs and test cases.
    Uses SQLite for reliable local storage.
    """

    def __init__(self, db_path: str = "./data/validations.db"):
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._initialize_schema()

    def _get_connection(self) -> sqlite3.Connection:
        """Get a database connection with row factory."""
        conn = sqlite3.connect(str(self.db_path))
        conn.row_factory = sqlite3.Row
        return conn

    def _initialize_schema(self) -> None:
        """Initialize the database schema."""
        with self._get_connection() as conn:
            cursor = conn.cursor()

            # Validation runs table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS validation_runs (
                    id TEXT PRIMARY KEY,
                    name TEXT NOT NULL,
                    description TEXT,
                    business_rules TEXT NOT NULL,
                    status TEXT NOT NULL DEFAULT 'pending',
                    total_tests INTEGER DEFAULT 0,
                    passed_tests INTEGER DEFAULT 0,
                    failed_tests INTEGER DEFAULT 0,
                    error_tests INTEGER DEFAULT 0,
                    skipped_tests INTEGER DEFAULT 0,
                    started_at TIMESTAMP,
                    completed_at TIMESTAMP,
                    execution_time_ms REAL DEFAULT 0,
                    created_by TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)

            # Test cases table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS test_cases (
                    id TEXT PRIMARY KEY,
                    run_id TEXT NOT NULL,
                    name TEXT NOT NULL,
                    description TEXT,
                    business_rule TEXT,
                    source_query TEXT,
                    target_query TEXT,
                    validation_type TEXT NOT NULL,
                    expected_result TEXT,
                    status TEXT NOT NULL DEFAULT 'pending',
                    actual_result TEXT,
                    error_message TEXT,
                    execution_time_ms REAL DEFAULT 0,
                    executed_at TIMESTAMP,
                    evidence TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (run_id) REFERENCES validation_runs(id) ON DELETE CASCADE
                )
            """)

            # Create indexes
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_test_cases_run_id 
                ON test_cases(run_id)
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_validation_runs_status 
                ON validation_runs(status)
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_validation_runs_created 
                ON validation_runs(created_at)
            """)

            conn.commit()

    def save_validation_run(self, run: ValidationRun) -> None:
        """Save or update a validation run."""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                INSERT OR REPLACE INTO validation_runs 
                (id, name, description, business_rules, status, total_tests,
                 passed_tests, failed_tests, error_tests, skipped_tests,
                 started_at, completed_at, execution_time_ms, created_by)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                run.id,
                run.name,
                run.description,
                json.dumps(run.business_rules),
                run.status.value,
                run.total_tests,
                run.passed_tests,
                run.failed_tests,
                run.error_tests,
                run.skipped_tests,
                run.started_at.isoformat() if run.started_at else None,
                run.completed_at.isoformat() if run.completed_at else None,
                run.execution_time_ms,
                run.created_by,
            ))
            conn.commit()

    def save_test_case(self, run_id: str, test_case: TestCase) -> None:
        """Save or update a test case."""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                INSERT OR REPLACE INTO test_cases 
                (id, run_id, name, description, business_rule, source_query,
                 target_query, validation_type, expected_result, status,
                 actual_result, error_message, execution_time_ms, executed_at, evidence)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                test_case.id,
                run_id,
                test_case.name,
                test_case.description,
                test_case.business_rule,
                test_case.source_query,
                test_case.target_query,
                test_case.validation_type,
                json.dumps(test_case.expected_result) if test_case.expected_result else None,
                test_case.status.value,
                json.dumps(test_case.actual_result) if test_case.actual_result else None,
                test_case.error_message,
                test_case.execution_time_ms,
                test_case.executed_at.isoformat() if test_case.executed_at else None,
                json.dumps(test_case.evidence) if test_case.evidence else None,
            ))
            conn.commit()

    def get_validation_run(self, run_id: str) -> ValidationRun | None:
        """Get a validation run by ID."""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM validation_runs WHERE id = ?", (run_id,))
            row = cursor.fetchone()
            if not row:
                return None

            # Get test cases
            cursor.execute(
                "SELECT * FROM test_cases WHERE run_id = ? ORDER BY created_at",
                (run_id,)
            )
            test_rows = cursor.fetchall()

            test_cases = [self._row_to_test_case(dict(r)) for r in test_rows]

            return ValidationRun(
                id=row["id"],
                name=row["name"],
                description=row["description"],
                business_rules=json.loads(row["business_rules"]),
                status=ValidationStatus(row["status"]),
                total_tests=row["total_tests"],
                passed_tests=row["passed_tests"],
                failed_tests=row["failed_tests"],
                error_tests=row["error_tests"],
                skipped_tests=row["skipped_tests"],
                started_at=datetime.fromisoformat(row["started_at"]) if row["started_at"] else None,
                completed_at=datetime.fromisoformat(row["completed_at"]) if row["completed_at"] else None,
                execution_time_ms=row["execution_time_ms"],
                created_by=row["created_by"],
                test_cases=test_cases,
            )

    def _row_to_test_case(self, row: dict) -> TestCase:
        """Convert a database row to a TestCase object."""
        return TestCase(
            id=row["id"],
            name=row["name"],
            description=row["description"] or "",
            business_rule=row["business_rule"] or "",
            source_query=row["source_query"],
            target_query=row["target_query"],
            validation_type=row["validation_type"],
            expected_result=json.loads(row["expected_result"]) if row["expected_result"] else None,
            status=ValidationStatus(row["status"]),
            actual_result=json.loads(row["actual_result"]) if row["actual_result"] else None,
            error_message=row["error_message"],
            execution_time_ms=row["execution_time_ms"],
            executed_at=datetime.fromisoformat(row["executed_at"]) if row["executed_at"] else None,
            evidence=json.loads(row["evidence"]) if row["evidence"] else {},
        )

    def list_validation_runs(
        self,
        limit: int = 50,
        offset: int = 0,
        status: ValidationStatus | None = None,
    ) -> list[ValidationRun]:
        """List validation runs with pagination."""
        with self._get_connection() as conn:
            cursor = conn.cursor()

            if status:
                cursor.execute("""
                    SELECT * FROM validation_runs 
                    WHERE status = ?
                    ORDER BY created_at DESC 
                    LIMIT ? OFFSET ?
                """, (status.value, limit, offset))
            else:
                cursor.execute("""
                    SELECT * FROM validation_runs 
                    ORDER BY created_at DESC 
                    LIMIT ? OFFSET ?
                """, (limit, offset))

            rows = cursor.fetchall()
            runs = []

            for row in rows:
                # Get test case count only (not full test cases for list view)
                run = ValidationRun(
                    id=row["id"],
                    name=row["name"],
                    description=row["description"],
                    business_rules=json.loads(row["business_rules"]),
                    status=ValidationStatus(row["status"]),
                    total_tests=row["total_tests"],
                    passed_tests=row["passed_tests"],
                    failed_tests=row["failed_tests"],
                    error_tests=row["error_tests"],
                    skipped_tests=row["skipped_tests"],
                    started_at=datetime.fromisoformat(row["started_at"]) if row["started_at"] else None,
                    completed_at=datetime.fromisoformat(row["completed_at"]) if row["completed_at"] else None,
                    execution_time_ms=row["execution_time_ms"],
                    created_by=row["created_by"],
                )
                runs.append(run)

            return runs

    def delete_validation_run(self, run_id: str) -> bool:
        """Delete a validation run and all associated test cases."""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("DELETE FROM validation_runs WHERE id = ?", (run_id,))
            conn.commit()
            return cursor.rowcount > 0

    def get_run_statistics(self) -> dict[str, Any]:
        """Get overall statistics for all validation runs."""
        with self._get_connection() as conn:
            cursor = conn.cursor()

            # Total runs by status
            cursor.execute("""
                SELECT status, COUNT(*) as count 
                FROM validation_runs 
                GROUP BY status
            """)
            status_counts = {row["status"]: row["count"] for row in cursor.fetchall()}

            # Total tests
            cursor.execute("""
                SELECT 
                    SUM(total_tests) as total,
                    SUM(passed_tests) as passed,
                    SUM(failed_tests) as failed,
                    SUM(error_tests) as errors
                FROM validation_runs
            """)
            test_stats = cursor.fetchone()

            # Recent runs
            cursor.execute("""
                SELECT COUNT(*) as count 
                FROM validation_runs 
                WHERE created_at > datetime('now', '-7 days')
            """)
            recent_count = cursor.fetchone()["count"]

            return {
                "total_runs": sum(status_counts.values()),
                "runs_by_status": status_counts,
                "total_tests": test_stats["total"] or 0,
                "passed_tests": test_stats["passed"] or 0,
                "failed_tests": test_stats["failed"] or 0,
                "error_tests": test_stats["errors"] or 0,
                "runs_last_7_days": recent_count,
            }
