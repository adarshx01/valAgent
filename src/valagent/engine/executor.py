"""
Test executor for running validation queries against databases.
Handles query execution, timing, and result collection.
"""

import asyncio
import logging
import time
from datetime import datetime
from typing import Any

from valagent.database import DatabaseManager, QueryResult, get_db_manager
from valagent.config import get_settings

logger = logging.getLogger(__name__)


class TestExecutor:
    """
    Executes validation queries against source and target databases.
    Provides detailed timing and result collection.
    """

    def __init__(self, db_manager: DatabaseManager | None = None):
        self.db = db_manager or get_db_manager()
        self.settings = get_settings()

    async def execute_query(
        self,
        query: str,
        database: str,
        timeout: int | None = None,
    ) -> QueryResult:
        """
        Execute a query on the specified database with timing.
        
        Args:
            query: SQL query to execute
            database: "source" or "target"
            timeout: Query timeout in seconds (optional)
            
        Returns:
            QueryResult with execution details
        """
        timeout = timeout or self.settings.db.query_timeout
        start_time = time.perf_counter()

        try:
            # Execute with timeout
            if database == "source":
                result = await asyncio.wait_for(
                    self.db.execute_source_query(query),
                    timeout=timeout,
                )
            elif database == "target":
                result = await asyncio.wait_for(
                    self.db.execute_target_query(query),
                    timeout=timeout,
                )
            else:
                raise ValueError(f"Invalid database: {database}")

            execution_time = (time.perf_counter() - start_time) * 1000  # ms

            # Extract columns from first row if available
            columns = list(result[0].keys()) if result else []

            return QueryResult(
                query=query,
                database=database,
                success=True,
                rows=result or [],
                row_count=len(result) if result else 0,
                columns=columns,
                execution_time_ms=execution_time,
                timestamp=datetime.utcnow(),
            )

        except asyncio.TimeoutError:
            execution_time = (time.perf_counter() - start_time) * 1000
            logger.error(f"Query timeout after {timeout}s on {database}")
            return QueryResult(
                query=query,
                database=database,
                success=False,
                error=f"Query timeout after {timeout} seconds",
                execution_time_ms=execution_time,
                timestamp=datetime.utcnow(),
            )

        except Exception as e:
            execution_time = (time.perf_counter() - start_time) * 1000
            logger.error(f"Query execution error on {database}: {e}")
            return QueryResult(
                query=query,
                database=database,
                success=False,
                error=str(e),
                execution_time_ms=execution_time,
                timestamp=datetime.utcnow(),
            )

    async def execute_count_query(
        self,
        table: str,
        database: str,
        where_clause: str | None = None,
    ) -> QueryResult:
        """
        Execute a count query on a table.
        """
        query = f"SELECT COUNT(*) as count FROM {table}"
        if where_clause:
            query += f" WHERE {where_clause}"

        return await self.execute_query(query, database)

    async def execute_comparison_queries(
        self,
        source_query: str | None,
        target_query: str,
        source_timeout: int | None = None,
        target_timeout: int | None = None,
    ) -> tuple[QueryResult | None, QueryResult]:
        """
        Execute queries on both source and target databases.
        Runs in parallel for efficiency.
        """
        tasks = []

        if source_query:
            tasks.append(
                self.execute_query(source_query, "source", source_timeout)
            )
        else:
            tasks.append(asyncio.coroutine(lambda: None)())

        tasks.append(
            self.execute_query(target_query, "target", target_timeout)
        )

        results = await asyncio.gather(*tasks, return_exceptions=True)

        source_result = None
        target_result = None

        if source_query:
            if isinstance(results[0], Exception):
                source_result = QueryResult(
                    query=source_query,
                    database="source",
                    success=False,
                    error=str(results[0]),
                    timestamp=datetime.utcnow(),
                )
            else:
                source_result = results[0]

        target_idx = 1 if source_query else 0
        if isinstance(results[target_idx], Exception):
            target_result = QueryResult(
                query=target_query,
                database="target",
                success=False,
                error=str(results[target_idx]),
                timestamp=datetime.utcnow(),
            )
        else:
            target_result = results[target_idx]

        return source_result, target_result

    async def execute_paginated_query(
        self,
        query: str,
        database: str,
        page_size: int | None = None,
        max_pages: int = 100,
    ) -> QueryResult:
        """
        Execute a query with pagination for large result sets.
        Aggregates results from multiple pages.
        """
        page_size = page_size or self.settings.db.batch_size
        all_rows = []
        total_time = 0
        page = 1
        columns = []

        while page <= max_pages:
            offset = (page - 1) * page_size
            paginated_query = f"{query} LIMIT {page_size} OFFSET {offset}"

            result = await self.execute_query(paginated_query, database)

            if not result.success:
                return result

            total_time += result.execution_time_ms

            if not result.rows:
                break

            if not columns:
                columns = result.columns

            all_rows.extend(result.rows)
            
            if len(result.rows) < page_size:
                break

            page += 1

            # Log progress for long operations
            if page % 10 == 0:
                logger.info(
                    f"Paginated query progress: {len(all_rows)} rows collected"
                )

        return QueryResult(
            query=query,
            database=database,
            success=True,
            rows=all_rows,
            row_count=len(all_rows),
            columns=columns,
            execution_time_ms=total_time,
            timestamp=datetime.utcnow(),
        )

    async def sample_data(
        self,
        table: str,
        database: str,
        sample_size: int = 100,
        order_by: str | None = None,
    ) -> QueryResult:
        """
        Get a sample of data from a table.
        """
        query = f"SELECT * FROM {table}"
        if order_by:
            query += f" ORDER BY {order_by}"
        query += f" LIMIT {sample_size}"

        return await self.execute_query(query, database)

    async def get_column_stats(
        self,
        table: str,
        column: str,
        database: str,
    ) -> QueryResult:
        """
        Get statistics for a specific column.
        """
        query = f"""
        SELECT 
            COUNT(*) as total_count,
            COUNT({column}) as non_null_count,
            COUNT(*) - COUNT({column}) as null_count,
            COUNT(DISTINCT {column}) as distinct_count
        FROM {table}
        """

        return await self.execute_query(query, database)

    async def check_duplicates(
        self,
        table: str,
        columns: list[str],
        database: str,
    ) -> QueryResult:
        """
        Check for duplicate values based on specified columns.
        """
        cols = ", ".join(columns)
        query = f"""
        SELECT {cols}, COUNT(*) as duplicate_count
        FROM {table}
        GROUP BY {cols}
        HAVING COUNT(*) > 1
        ORDER BY COUNT(*) DESC
        LIMIT 100
        """

        return await self.execute_query(query, database)

    async def check_referential_integrity(
        self,
        parent_table: str,
        parent_key: str,
        child_table: str,
        child_key: str,
        database: str,
    ) -> QueryResult:
        """
        Check for orphan records (referential integrity violations).
        """
        query = f"""
        SELECT c.*
        FROM {child_table} c
        LEFT JOIN {parent_table} p ON c.{child_key} = p.{parent_key}
        WHERE p.{parent_key} IS NULL
        LIMIT 100
        """

        return await self.execute_query(query, database)

    async def validate_not_null(
        self,
        table: str,
        column: str,
        database: str,
    ) -> QueryResult:
        """
        Check for NULL values in a column that should be NOT NULL.
        """
        query = f"""
        SELECT COUNT(*) as null_count
        FROM {table}
        WHERE {column} IS NULL
        """

        return await self.execute_query(query, database)

    async def validate_range(
        self,
        table: str,
        column: str,
        min_value: Any,
        max_value: Any,
        database: str,
    ) -> QueryResult:
        """
        Check for values outside an expected range.
        """
        query = f"""
        SELECT *
        FROM {table}
        WHERE {column} < {min_value} OR {column} > {max_value}
        LIMIT 100
        """

        return await self.execute_query(query, database)
