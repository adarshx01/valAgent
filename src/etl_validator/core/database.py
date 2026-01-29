"""
Database connection and management module.

Provides async database connections with connection pooling and parallel query execution.
Uses asyncpg for high-performance PostgreSQL operations.
"""

import asyncio
from contextlib import asynccontextmanager
from typing import Any, AsyncGenerator
import asyncpg
from asyncpg import Pool, Connection, Record

from .config import settings
from .exceptions import DatabaseConnectionError, QueryExecutionError
from ..utils.logger import get_logger

logger = get_logger(__name__)


class DatabaseManager:
    """
    Manages database connections with connection pooling for both source and target databases.
    
    Supports parallel query execution for handling large datasets efficiently.
    """

    def __init__(self):
        self._source_pool: Pool | None = None
        self._target_pool: Pool | None = None
        self._initialized = False

    async def initialize(self) -> None:
        """Initialize database connection pools."""
        if self._initialized:
            return

        try:
            logger.info("Initializing database connection pools...")

            # Create source database pool
            self._source_pool = await asyncpg.create_pool(
                dsn=settings.source_db_uri.get_secret_value(),
                min_size=5,
                max_size=settings.db_pool_size,
                max_inactive_connection_lifetime=settings.db_pool_recycle,
                command_timeout=settings.query_timeout,
                statement_cache_size=100,
            )
            logger.info("Source database pool created successfully")

            # Create target database pool
            self._target_pool = await asyncpg.create_pool(
                dsn=settings.target_db_uri.get_secret_value(),
                min_size=5,
                max_size=settings.db_pool_size,
                max_inactive_connection_lifetime=settings.db_pool_recycle,
                command_timeout=settings.query_timeout,
                statement_cache_size=100,
            )
            logger.info("Target database pool created successfully")

            self._initialized = True
            logger.info("Database manager initialized successfully")

        except Exception as e:
            logger.error(f"Failed to initialize database pools: {e}")
            raise DatabaseConnectionError(
                message="Failed to initialize database connection pools",
                database="source/target",
                details={"error": str(e)},
            )

    async def close(self) -> None:
        """Close all database connection pools."""
        if self._source_pool:
            await self._source_pool.close()
            logger.info("Source database pool closed")
        if self._target_pool:
            await self._target_pool.close()
            logger.info("Target database pool closed")
        self._initialized = False

    @asynccontextmanager
    async def get_source_connection(self) -> AsyncGenerator[Connection, None]:
        """Get a connection from the source database pool."""
        if not self._source_pool:
            raise DatabaseConnectionError(
                message="Source database pool not initialized",
                database="source",
            )
        async with self._source_pool.acquire() as conn:
            yield conn

    @asynccontextmanager
    async def get_target_connection(self) -> AsyncGenerator[Connection, None]:
        """Get a connection from the target database pool."""
        if not self._target_pool:
            raise DatabaseConnectionError(
                message="Target database pool not initialized",
                database="target",
            )
        async with self._target_pool.acquire() as conn:
            yield conn

    async def execute_source_query(
        self,
        query: str,
        params: tuple | None = None,
        timeout: float | None = None,
    ) -> list[Record]:
        """Execute a query on the source database."""
        return await self._execute_query(
            pool=self._source_pool,
            query=query,
            params=params,
            timeout=timeout,
            database="source",
        )

    async def execute_target_query(
        self,
        query: str,
        params: tuple | None = None,
        timeout: float | None = None,
    ) -> list[Record]:
        """Execute a query on the target database."""
        return await self._execute_query(
            pool=self._target_pool,
            query=query,
            params=params,
            timeout=timeout,
            database="target",
        )

    async def _execute_query(
        self,
        pool: Pool | None,
        query: str,
        params: tuple | None = None,
        timeout: float | None = None,
        database: str = "unknown",
    ) -> list[Record]:
        """Execute a query with error handling."""
        if not pool:
            raise DatabaseConnectionError(
                message=f"{database.capitalize()} database pool not initialized",
                database=database,
            )

        try:
            async with pool.acquire() as conn:
                if params:
                    result = await conn.fetch(query, *params, timeout=timeout)
                else:
                    result = await conn.fetch(query, timeout=timeout)
                return result
        except asyncpg.PostgresError as e:
            logger.error(f"Query execution failed on {database}: {e}")
            raise QueryExecutionError(
                message=f"Query execution failed on {database} database",
                query=query[:500],  # Truncate for logging
                database=database,
                details={"error": str(e), "error_type": type(e).__name__},
            )
        except asyncio.TimeoutError:
            logger.error(f"Query timeout on {database} database")
            raise QueryExecutionError(
                message=f"Query timeout on {database} database",
                query=query[:500],
                database=database,
                details={"timeout": timeout or settings.query_timeout},
            )

    async def execute_parallel_queries(
        self,
        queries: list[dict[str, Any]],
        max_concurrent: int | None = None,
    ) -> list[dict[str, Any]]:
        """
        Execute multiple queries in parallel with controlled concurrency.
        
        Args:
            queries: List of dicts with 'query', 'database' ('source' or 'target'), 
                    and optional 'params' and 'id' keys
            max_concurrent: Maximum concurrent queries (defaults to settings.max_parallel_workers)
            
        Returns:
            List of results with 'id', 'success', 'data' or 'error' keys
        """
        max_concurrent = max_concurrent or settings.max_parallel_workers
        semaphore = asyncio.Semaphore(max_concurrent)

        async def execute_single(query_info: dict) -> dict[str, Any]:
            async with semaphore:
                query_id = query_info.get("id", "unknown")
                query = query_info["query"]
                database = query_info.get("database", "source")
                params = query_info.get("params")

                try:
                    if database == "source":
                        result = await self.execute_source_query(query, params)
                    else:
                        result = await self.execute_target_query(query, params)

                    return {
                        "id": query_id,
                        "success": True,
                        "data": [dict(r) for r in result],
                        "row_count": len(result),
                    }
                except Exception as e:
                    logger.error(f"Parallel query {query_id} failed: {e}")
                    return {
                        "id": query_id,
                        "success": False,
                        "error": str(e),
                    }

        tasks = [execute_single(q) for q in queries]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        # Handle any unexpected exceptions
        processed_results = []
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                processed_results.append({
                    "id": queries[i].get("id", i),
                    "success": False,
                    "error": str(result),
                })
            else:
                processed_results.append(result)

        return processed_results

    async def get_source_schema_info(self) -> dict[str, Any]:
        """Get comprehensive schema information from source database."""
        return await self._get_schema_info(self._source_pool, "source")

    async def get_target_schema_info(self) -> dict[str, Any]:
        """Get comprehensive schema information from target database."""
        return await self._get_schema_info(self._target_pool, "target")

    async def _get_schema_info(self, pool: Pool | None, database: str) -> dict[str, Any]:
        """Extract comprehensive schema information from a database."""
        if not pool:
            raise DatabaseConnectionError(
                message=f"{database.capitalize()} database pool not initialized",
                database=database,
            )

        async with pool.acquire() as conn:
            # Get all tables with their schemas
            tables_query = """
                SELECT 
                    t.table_schema,
                    t.table_name,
                    t.table_type,
                    pg_catalog.obj_description(
                        (quote_ident(t.table_schema) || '.' || quote_ident(t.table_name))::regclass, 
                        'pg_class'
                    ) as table_comment
                FROM information_schema.tables t
                WHERE t.table_schema NOT IN ('pg_catalog', 'information_schema')
                ORDER BY t.table_schema, t.table_name;
            """
            tables = await conn.fetch(tables_query)

            # Get all columns with detailed info
            columns_query = """
                SELECT 
                    c.table_schema,
                    c.table_name,
                    c.column_name,
                    c.ordinal_position,
                    c.column_default,
                    c.is_nullable,
                    c.data_type,
                    c.character_maximum_length,
                    c.numeric_precision,
                    c.numeric_scale,
                    c.udt_name,
                    pg_catalog.col_description(
                        (quote_ident(c.table_schema) || '.' || quote_ident(c.table_name))::regclass,
                        c.ordinal_position
                    ) as column_comment
                FROM information_schema.columns c
                WHERE c.table_schema NOT IN ('pg_catalog', 'information_schema')
                ORDER BY c.table_schema, c.table_name, c.ordinal_position;
            """
            columns = await conn.fetch(columns_query)

            # Get primary keys
            pk_query = """
                SELECT 
                    tc.table_schema,
                    tc.table_name,
                    kcu.column_name,
                    tc.constraint_name
                FROM information_schema.table_constraints tc
                JOIN information_schema.key_column_usage kcu 
                    ON tc.constraint_name = kcu.constraint_name
                    AND tc.table_schema = kcu.table_schema
                WHERE tc.constraint_type = 'PRIMARY KEY'
                    AND tc.table_schema NOT IN ('pg_catalog', 'information_schema')
                ORDER BY tc.table_schema, tc.table_name, kcu.ordinal_position;
            """
            primary_keys = await conn.fetch(pk_query)

            # Get foreign keys
            fk_query = """
                SELECT
                    tc.table_schema,
                    tc.table_name,
                    kcu.column_name,
                    ccu.table_schema AS foreign_table_schema,
                    ccu.table_name AS foreign_table_name,
                    ccu.column_name AS foreign_column_name,
                    tc.constraint_name
                FROM information_schema.table_constraints AS tc
                JOIN information_schema.key_column_usage AS kcu
                    ON tc.constraint_name = kcu.constraint_name
                    AND tc.table_schema = kcu.table_schema
                JOIN information_schema.constraint_column_usage AS ccu
                    ON ccu.constraint_name = tc.constraint_name
                    AND ccu.table_schema = tc.table_schema
                WHERE tc.constraint_type = 'FOREIGN KEY'
                    AND tc.table_schema NOT IN ('pg_catalog', 'information_schema');
            """
            foreign_keys = await conn.fetch(fk_query)

            # Get indexes
            indexes_query = """
                SELECT
                    schemaname as table_schema,
                    tablename as table_name,
                    indexname as index_name,
                    indexdef as index_definition
                FROM pg_indexes
                WHERE schemaname NOT IN ('pg_catalog', 'information_schema')
                ORDER BY schemaname, tablename, indexname;
            """
            indexes = await conn.fetch(indexes_query)

            # Get row counts for all tables (approximate for performance)
            row_counts_query = """
                SELECT
                    schemaname as table_schema,
                    relname as table_name,
                    n_live_tup as approximate_row_count
                FROM pg_stat_user_tables
                ORDER BY schemaname, relname;
            """
            row_counts = await conn.fetch(row_counts_query)

            # Organize data into structured format
            schema_info = {
                "database": database,
                "tables": {},
                "summary": {
                    "total_tables": len(tables),
                    "total_columns": len(columns),
                },
            }

            # Build table structure
            for table in tables:
                table_key = f"{table['table_schema']}.{table['table_name']}"
                schema_info["tables"][table_key] = {
                    "schema": table["table_schema"],
                    "name": table["table_name"],
                    "type": table["table_type"],
                    "comment": table["table_comment"],
                    "columns": [],
                    "primary_keys": [],
                    "foreign_keys": [],
                    "indexes": [],
                    "approximate_row_count": 0,
                }

            # Add columns
            for col in columns:
                table_key = f"{col['table_schema']}.{col['table_name']}"
                if table_key in schema_info["tables"]:
                    schema_info["tables"][table_key]["columns"].append({
                        "name": col["column_name"],
                        "position": col["ordinal_position"],
                        "default": col["column_default"],
                        "nullable": col["is_nullable"] == "YES",
                        "data_type": col["data_type"],
                        "max_length": col["character_maximum_length"],
                        "precision": col["numeric_precision"],
                        "scale": col["numeric_scale"],
                        "udt_name": col["udt_name"],
                        "comment": col["column_comment"],
                    })

            # Add primary keys
            for pk in primary_keys:
                table_key = f"{pk['table_schema']}.{pk['table_name']}"
                if table_key in schema_info["tables"]:
                    schema_info["tables"][table_key]["primary_keys"].append(pk["column_name"])

            # Add foreign keys
            for fk in foreign_keys:
                table_key = f"{fk['table_schema']}.{fk['table_name']}"
                if table_key in schema_info["tables"]:
                    schema_info["tables"][table_key]["foreign_keys"].append({
                        "column": fk["column_name"],
                        "references_schema": fk["foreign_table_schema"],
                        "references_table": fk["foreign_table_name"],
                        "references_column": fk["foreign_column_name"],
                        "constraint_name": fk["constraint_name"],
                    })

            # Add indexes
            for idx in indexes:
                table_key = f"{idx['table_schema']}.{idx['table_name']}"
                if table_key in schema_info["tables"]:
                    schema_info["tables"][table_key]["indexes"].append({
                        "name": idx["index_name"],
                        "definition": idx["index_definition"],
                    })

            # Add row counts
            for rc in row_counts:
                table_key = f"{rc['table_schema']}.{rc['table_name']}"
                if table_key in schema_info["tables"]:
                    schema_info["tables"][table_key]["approximate_row_count"] = rc["approximate_row_count"]

            return schema_info


# Global database manager instance
db_manager = DatabaseManager()
