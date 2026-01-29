"""
Schema Service for database schema extraction and analysis.

Handles schema extraction, comparison, and context generation.
"""

from typing import Any
from datetime import datetime, timezone

from ..core.database import DatabaseManager
from ..core.exceptions import SchemaExtractionError
from ..models.schema import (
    DatabaseSchema,
    TableInfo,
    ColumnInfo,
    ForeignKeyInfo,
    IndexInfo,
    SchemaComparisonResult,
    TableDifference,
    ColumnDifference,
)
from ..utils.logger import get_logger
from ..utils.helpers import get_timestamp_str

logger = get_logger(__name__)


class SchemaService:
    """
    Service for database schema operations.
    
    Extracts, caches, and compares database schemas.
    """

    def __init__(self, db_manager: DatabaseManager):
        self._db_manager = db_manager
        self._source_schema_cache: DatabaseSchema | None = None
        self._target_schema_cache: DatabaseSchema | None = None

    async def get_source_schema(self, force_refresh: bool = False) -> DatabaseSchema:
        """
        Get source database schema.
        
        Args:
            force_refresh: Force refresh from database
            
        Returns:
            Source database schema
        """
        if self._source_schema_cache and not force_refresh:
            return self._source_schema_cache

        try:
            logger.info("Extracting source database schema...")
            schema_info = await self._db_manager.get_source_schema_info()
            self._source_schema_cache = self._build_schema_model(schema_info, "source")
            logger.info(
                f"Source schema extracted: {len(self._source_schema_cache.tables)} tables"
            )
            return self._source_schema_cache

        except Exception as e:
            logger.error(f"Failed to extract source schema: {e}")
            raise SchemaExtractionError(
                message="Failed to extract source database schema",
                database="source",
                details={"error": str(e)},
            )

    async def get_target_schema(self, force_refresh: bool = False) -> DatabaseSchema:
        """
        Get target database schema.
        
        Args:
            force_refresh: Force refresh from database
            
        Returns:
            Target database schema
        """
        if self._target_schema_cache and not force_refresh:
            return self._target_schema_cache

        try:
            logger.info("Extracting target database schema...")
            schema_info = await self._db_manager.get_target_schema_info()
            self._target_schema_cache = self._build_schema_model(schema_info, "target")
            logger.info(
                f"Target schema extracted: {len(self._target_schema_cache.tables)} tables"
            )
            return self._target_schema_cache

        except Exception as e:
            logger.error(f"Failed to extract target schema: {e}")
            raise SchemaExtractionError(
                message="Failed to extract target database schema",
                database="target",
                details={"error": str(e)},
            )

    async def get_both_schemas(
        self, force_refresh: bool = False
    ) -> tuple[DatabaseSchema, DatabaseSchema]:
        """
        Get both source and target schemas.
        
        Args:
            force_refresh: Force refresh from database
            
        Returns:
            Tuple of (source_schema, target_schema)
        """
        source_schema = await self.get_source_schema(force_refresh)
        target_schema = await self.get_target_schema(force_refresh)
        return source_schema, target_schema

    def _build_schema_model(
        self, schema_info: dict[str, Any], database_name: str
    ) -> DatabaseSchema:
        """Build DatabaseSchema model from raw schema info."""
        tables = {}

        for table_key, table_data in schema_info.get("tables", {}).items():
            # Build columns
            columns = [
                ColumnInfo(
                    name=col["name"],
                    position=col["position"],
                    data_type=col["data_type"],
                    udt_name=col.get("udt_name"),
                    nullable=col.get("nullable", True),
                    default=col.get("default"),
                    max_length=col.get("max_length"),
                    precision=col.get("precision"),
                    scale=col.get("scale"),
                    comment=col.get("comment"),
                )
                for col in table_data.get("columns", [])
            ]

            # Build foreign keys
            foreign_keys = [
                ForeignKeyInfo(
                    column=fk["column"],
                    references_schema=fk["references_schema"],
                    references_table=fk["references_table"],
                    references_column=fk["references_column"],
                    constraint_name=fk["constraint_name"],
                )
                for fk in table_data.get("foreign_keys", [])
            ]

            # Build indexes
            indexes = [
                IndexInfo(name=idx["name"], definition=idx["definition"])
                for idx in table_data.get("indexes", [])
            ]

            table_info = TableInfo(
                schema_name=table_data["schema"],
                table_name=table_data["name"],
                table_type=table_data.get("type", "BASE TABLE"),
                comment=table_data.get("comment"),
                columns=columns,
                primary_keys=table_data.get("primary_keys", []),
                foreign_keys=foreign_keys,
                indexes=indexes,
                approximate_row_count=table_data.get("approximate_row_count", 0),
            )

            tables[table_key] = table_info

        return DatabaseSchema(
            database_name=database_name,
            tables=tables,
            extraction_timestamp=get_timestamp_str(),
            summary=schema_info.get("summary", {}),
        )

    async def compare_schemas(self) -> SchemaComparisonResult:
        """
        Compare source and target schemas.
        
        Returns:
            Schema comparison result with differences
        """
        source_schema = await self.get_source_schema()
        target_schema = await self.get_target_schema()

        differences = []
        matching_tables = []

        source_tables = set(source_schema.tables.keys())
        target_tables = set(target_schema.tables.keys())

        # Find tables only in source
        for table_name in source_tables - target_tables:
            differences.append(
                TableDifference(
                    table_name=table_name,
                    difference_type="missing_in_target",
                    description=f"Table {table_name} exists in source but not in target",
                )
            )

        # Find tables only in target
        for table_name in target_tables - source_tables:
            differences.append(
                TableDifference(
                    table_name=table_name,
                    difference_type="missing_in_source",
                    description=f"Table {table_name} exists in target but not in source",
                )
            )

        # Compare common tables
        for table_name in source_tables & target_tables:
            source_table = source_schema.tables[table_name]
            target_table = target_schema.tables[table_name]

            column_diffs = self._compare_columns(source_table, target_table)

            if column_diffs:
                differences.append(
                    TableDifference(
                        table_name=table_name,
                        difference_type="column_differences",
                        column_differences=column_diffs,
                        description=f"Table {table_name} has {len(column_diffs)} column differences",
                    )
                )
            else:
                matching_tables.append(table_name)

        return SchemaComparisonResult(
            source_database="source",
            target_database="target",
            comparison_timestamp=get_timestamp_str(),
            is_compatible=len(differences) == 0,
            differences=differences,
            matching_tables=matching_tables,
            summary={
                "total_source_tables": len(source_tables),
                "total_target_tables": len(target_tables),
                "matching_tables": len(matching_tables),
                "tables_with_differences": len(
                    [d for d in differences if d.difference_type == "column_differences"]
                ),
                "tables_missing_in_target": len(
                    [d for d in differences if d.difference_type == "missing_in_target"]
                ),
                "tables_missing_in_source": len(
                    [d for d in differences if d.difference_type == "missing_in_source"]
                ),
            },
        )

    def _compare_columns(
        self, source_table: TableInfo, target_table: TableInfo
    ) -> list[ColumnDifference]:
        """Compare columns between source and target tables."""
        differences = []

        source_cols = {col.name.lower(): col for col in source_table.columns}
        target_cols = {col.name.lower(): col for col in target_table.columns}

        # Columns only in source
        for col_name in set(source_cols.keys()) - set(target_cols.keys()):
            differences.append(
                ColumnDifference(
                    column_name=source_cols[col_name].name,
                    difference_type="missing_in_target",
                    source_value=source_cols[col_name].data_type,
                    description=f"Column {source_cols[col_name].name} missing in target",
                )
            )

        # Columns only in target
        for col_name in set(target_cols.keys()) - set(source_cols.keys()):
            differences.append(
                ColumnDifference(
                    column_name=target_cols[col_name].name,
                    difference_type="missing_in_source",
                    target_value=target_cols[col_name].data_type,
                    description=f"Column {target_cols[col_name].name} missing in source",
                )
            )

        # Compare common columns
        for col_name in set(source_cols.keys()) & set(target_cols.keys()):
            source_col = source_cols[col_name]
            target_col = target_cols[col_name]

            # Check data type
            if source_col.data_type.lower() != target_col.data_type.lower():
                differences.append(
                    ColumnDifference(
                        column_name=source_col.name,
                        difference_type="type_mismatch",
                        source_value=source_col.data_type,
                        target_value=target_col.data_type,
                        description=f"Column {source_col.name} type mismatch: {source_col.data_type} vs {target_col.data_type}",
                    )
                )

            # Check nullable
            if source_col.nullable != target_col.nullable:
                differences.append(
                    ColumnDifference(
                        column_name=source_col.name,
                        difference_type="nullable_mismatch",
                        source_value=str(source_col.nullable),
                        target_value=str(target_col.nullable),
                        description=f"Column {source_col.name} nullable mismatch",
                    )
                )

        return differences

    def clear_cache(self) -> None:
        """Clear schema cache."""
        self._source_schema_cache = None
        self._target_schema_cache = None
        logger.info("Schema cache cleared")
