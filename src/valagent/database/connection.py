"""
Database connection management with connection pooling and async support.
Handles connections to both source and target PostgreSQL databases.
"""

import asyncio
from contextlib import asynccontextmanager
from functools import lru_cache
from typing import Any, AsyncGenerator
import logging

from sqlalchemy import text, create_engine, MetaData, inspect
from sqlalchemy.ext.asyncio import (
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
    AsyncEngine,
)
from sqlalchemy.pool import QueuePool

from valagent.config import get_settings

logger = logging.getLogger(__name__)


class DatabaseManager:
    """
    Manages database connections with connection pooling for both source and target databases.
    Supports both sync and async operations for flexibility.
    """

    def __init__(self):
        self.settings = get_settings()
        self._source_engine: AsyncEngine | None = None
        self._target_engine: AsyncEngine | None = None
        self._source_sync_engine = None
        self._target_sync_engine = None
        self._source_session_factory: async_sessionmaker | None = None
        self._target_session_factory: async_sessionmaker | None = None
        self._initialized = False

    def _convert_to_async_uri(self, uri: str) -> str:
        """Convert a sync PostgreSQL URI to async (asyncpg) URI."""
        if uri.startswith("postgresql://"):
            return uri.replace("postgresql://", "postgresql+asyncpg://", 1)
        elif uri.startswith("postgres://"):
            return uri.replace("postgres://", "postgresql+asyncpg://", 1)
        return uri

    async def initialize(self) -> None:
        """Initialize database connections and connection pools."""
        if self._initialized:
            return

        source_uri = self.settings.db.source_db_uri.get_secret_value()
        target_uri = self.settings.db.target_db_uri.get_secret_value()

        # Create async engines
        async_source_uri = self._convert_to_async_uri(source_uri)
        async_target_uri = self._convert_to_async_uri(target_uri)

        pool_config = {
            "pool_size": self.settings.db.db_pool_size,
            "max_overflow": self.settings.db.db_max_overflow,
            "pool_timeout": self.settings.db.db_pool_timeout,
            "pool_recycle": self.settings.db.db_pool_recycle,
            "pool_pre_ping": True,
        }

        self._source_engine = create_async_engine(
            async_source_uri,
            echo=self.settings.app.debug,
            **pool_config,
        )

        self._target_engine = create_async_engine(
            async_target_uri,
            echo=self.settings.app.debug,
            **pool_config,
        )

        # Create sync engines for schema introspection
        self._source_sync_engine = create_engine(
            source_uri,
            poolclass=QueuePool,
            pool_size=5,
            max_overflow=10,
            pool_pre_ping=True,
        )

        self._target_sync_engine = create_engine(
            target_uri,
            poolclass=QueuePool,
            pool_size=5,
            max_overflow=10,
            pool_pre_ping=True,
        )

        # Create session factories
        self._source_session_factory = async_sessionmaker(
            bind=self._source_engine,
            class_=AsyncSession,
            expire_on_commit=False,
        )

        self._target_session_factory = async_sessionmaker(
            bind=self._target_engine,
            class_=AsyncSession,
            expire_on_commit=False,
        )

        self._initialized = True
        logger.info("Database connections initialized successfully")

    async def close(self) -> None:
        """Close all database connections."""
        if self._source_engine:
            await self._source_engine.dispose()
        if self._target_engine:
            await self._target_engine.dispose()
        if self._source_sync_engine:
            self._source_sync_engine.dispose()
        if self._target_sync_engine:
            self._target_sync_engine.dispose()
        self._initialized = False
        logger.info("Database connections closed")

    @asynccontextmanager
    async def source_session(self) -> AsyncGenerator[AsyncSession, None]:
        """Get an async session for the source database."""
        if not self._initialized:
            await self.initialize()
        async with self._source_session_factory() as session:
            try:
                yield session
                await session.commit()
            except Exception:
                await session.rollback()
                raise

    @asynccontextmanager
    async def target_session(self) -> AsyncGenerator[AsyncSession, None]:
        """Get an async session for the target database."""
        if not self._initialized:
            await self.initialize()
        async with self._target_session_factory() as session:
            try:
                yield session
                await session.commit()
            except Exception:
                await session.rollback()
                raise

    async def execute_source_query(
        self,
        query: str,
        params: dict[str, Any] | None = None,
        fetch_all: bool = True,
    ) -> list[dict[str, Any]] | dict[str, Any] | None:
        """Execute a query on the source database."""
        async with self.source_session() as session:
            result = await session.execute(
                text(query),
                params or {},
            )
            if fetch_all:
                rows = result.fetchall()
                columns = result.keys()
                return [dict(zip(columns, row)) for row in rows]
            else:
                row = result.fetchone()
                if row:
                    columns = result.keys()
                    return dict(zip(columns, row))
                return None

    async def execute_target_query(
        self,
        query: str,
        params: dict[str, Any] | None = None,
        fetch_all: bool = True,
    ) -> list[dict[str, Any]] | dict[str, Any] | None:
        """Execute a query on the target database."""
        async with self.target_session() as session:
            result = await session.execute(
                text(query),
                params or {},
            )
            if fetch_all:
                rows = result.fetchall()
                columns = result.keys()
                return [dict(zip(columns, row)) for row in rows]
            else:
                row = result.fetchone()
                if row:
                    columns = result.keys()
                    return dict(zip(columns, row))
                return None

    async def execute_query_with_pagination(
        self,
        query: str,
        database: str,
        page: int = 1,
        page_size: int | None = None,
    ) -> tuple[list[dict[str, Any]], int]:
        """
        Execute a query with pagination support for large datasets.
        Returns (results, total_count).
        """
        if page_size is None:
            page_size = self.settings.db.batch_size

        offset = (page - 1) * page_size

        # Get total count
        count_query = f"SELECT COUNT(*) as total FROM ({query}) as subquery"
        if database == "source":
            count_result = await self.execute_source_query(count_query, fetch_all=False)
        else:
            count_result = await self.execute_target_query(count_query, fetch_all=False)

        total_count = count_result["total"] if count_result else 0

        # Get paginated results
        paginated_query = f"{query} LIMIT {page_size} OFFSET {offset}"
        if database == "source":
            results = await self.execute_source_query(paginated_query)
        else:
            results = await self.execute_target_query(paginated_query)

        return results or [], total_count

    def get_source_schema(self) -> dict[str, Any]:
        """Get schema information from the source database (sync operation)."""
        return self._get_schema(self._source_sync_engine)

    def get_target_schema(self) -> dict[str, Any]:
        """Get schema information from the target database (sync operation)."""
        return self._get_schema(self._target_sync_engine)

    def _get_schema(self, engine) -> dict[str, Any]:
        """Extract comprehensive schema information from a database."""
        inspector = inspect(engine)
        schema_info = {
            "tables": {},
            "views": [],
            "schemas": inspector.get_schema_names(),
        }

        # Get default schema tables
        for table_name in inspector.get_table_names():
            columns = []
            for column in inspector.get_columns(table_name):
                columns.append({
                    "name": column["name"],
                    "type": str(column["type"]),
                    "nullable": column.get("nullable", True),
                    "default": str(column.get("default")) if column.get("default") else None,
                    "primary_key": False,  # Will be updated below
                })

            # Get primary keys
            pk_constraint = inspector.get_pk_constraint(table_name)
            pk_columns = pk_constraint.get("constrained_columns", [])
            for col in columns:
                if col["name"] in pk_columns:
                    col["primary_key"] = True

            # Get foreign keys
            foreign_keys = []
            for fk in inspector.get_foreign_keys(table_name):
                foreign_keys.append({
                    "columns": fk["constrained_columns"],
                    "referred_table": fk["referred_table"],
                    "referred_columns": fk["referred_columns"],
                })

            # Get indexes
            indexes = []
            for idx in inspector.get_indexes(table_name):
                indexes.append({
                    "name": idx["name"],
                    "columns": idx["column_names"],
                    "unique": idx.get("unique", False),
                })

            schema_info["tables"][table_name] = {
                "columns": columns,
                "primary_keys": pk_columns,
                "foreign_keys": foreign_keys,
                "indexes": indexes,
                "row_count": None,  # Will be populated on demand
            }

        # Get views
        schema_info["views"] = inspector.get_view_names()

        return schema_info

    async def get_table_row_count(
        self, table_name: str, database: str
    ) -> int:
        """Get the row count for a specific table."""
        query = f"SELECT COUNT(*) as count FROM {table_name}"
        if database == "source":
            result = await self.execute_source_query(query, fetch_all=False)
        else:
            result = await self.execute_target_query(query, fetch_all=False)
        return result["count"] if result else 0

    async def get_table_sample(
        self,
        table_name: str,
        database: str,
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        """Get a sample of rows from a table."""
        query = f"SELECT * FROM {table_name} LIMIT {limit}"
        if database == "source":
            return await self.execute_source_query(query) or []
        else:
            return await self.execute_target_query(query) or []

    async def test_connection(self, database: str) -> tuple[bool, str]:
        """Test connection to a database."""
        try:
            query = "SELECT 1 as test"
            if database == "source":
                result = await self.execute_source_query(query, fetch_all=False)
            else:
                result = await self.execute_target_query(query, fetch_all=False)

            if result and result.get("test") == 1:
                return True, "Connection successful"
            return False, "Unexpected query result"
        except Exception as e:
            logger.error(f"Connection test failed for {database}: {e}")
            return False, str(e)


# Singleton instance
_db_manager: DatabaseManager | None = None


@lru_cache
def get_db_manager() -> DatabaseManager:
    """Get or create the database manager singleton."""
    global _db_manager
    if _db_manager is None:
        _db_manager = DatabaseManager()
    return _db_manager
