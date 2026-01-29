"""
Schema models for representing database structures.

These models capture the complete database schema information
for both source and target databases.
"""

from typing import Any
from pydantic import BaseModel, Field


class ColumnInfo(BaseModel):
    """Represents a database column."""

    name: str = Field(..., description="Column name")
    position: int = Field(..., description="Ordinal position")
    data_type: str = Field(..., description="Data type")
    udt_name: str | None = Field(None, description="User-defined type name")
    nullable: bool = Field(True, description="Whether column allows NULL")
    default: str | None = Field(None, description="Default value")
    max_length: int | None = Field(None, description="Maximum character length")
    precision: int | None = Field(None, description="Numeric precision")
    scale: int | None = Field(None, description="Numeric scale")
    comment: str | None = Field(None, description="Column comment")

    def get_full_type(self) -> str:
        """Get full type string including precision/scale."""
        if self.max_length:
            return f"{self.data_type}({self.max_length})"
        elif self.precision and self.scale:
            return f"{self.data_type}({self.precision},{self.scale})"
        elif self.precision:
            return f"{self.data_type}({self.precision})"
        return self.data_type


class ForeignKeyInfo(BaseModel):
    """Represents a foreign key constraint."""

    column: str = Field(..., description="Local column name")
    references_schema: str = Field(..., description="Referenced schema")
    references_table: str = Field(..., description="Referenced table")
    references_column: str = Field(..., description="Referenced column")
    constraint_name: str = Field(..., description="Constraint name")


class IndexInfo(BaseModel):
    """Represents a database index."""

    name: str = Field(..., description="Index name")
    definition: str = Field(..., description="Index definition SQL")


class TableInfo(BaseModel):
    """Represents a database table with complete metadata."""

    schema_name: str = Field(..., description="Schema name")
    table_name: str = Field(..., description="Table name")
    table_type: str = Field("BASE TABLE", description="Table type")
    comment: str | None = Field(None, description="Table comment")
    columns: list[ColumnInfo] = Field(default_factory=list, description="Table columns")
    primary_keys: list[str] = Field(default_factory=list, description="Primary key columns")
    foreign_keys: list[ForeignKeyInfo] = Field(default_factory=list, description="Foreign keys")
    indexes: list[IndexInfo] = Field(default_factory=list, description="Table indexes")
    approximate_row_count: int = Field(0, description="Approximate row count")

    @property
    def full_name(self) -> str:
        """Get fully qualified table name."""
        return f"{self.schema_name}.{self.table_name}"

    def get_column(self, name: str) -> ColumnInfo | None:
        """Get column by name."""
        for col in self.columns:
            if col.name.lower() == name.lower():
                return col
        return None

    def get_column_names(self) -> list[str]:
        """Get list of column names."""
        return [col.name for col in self.columns]

    def to_ddl_summary(self) -> str:
        """Generate DDL-like summary for LLM context."""
        lines = [f"Table: {self.full_name}"]
        if self.comment:
            lines.append(f"  Comment: {self.comment}")
        lines.append(f"  Approximate rows: {self.approximate_row_count:,}")
        lines.append("  Columns:")
        for col in self.columns:
            nullable = "NULL" if col.nullable else "NOT NULL"
            default = f" DEFAULT {col.default}" if col.default else ""
            pk_marker = " (PK)" if col.name in self.primary_keys else ""
            lines.append(f"    - {col.name}: {col.get_full_type()} {nullable}{default}{pk_marker}")
        if self.foreign_keys:
            lines.append("  Foreign Keys:")
            for fk in self.foreign_keys:
                lines.append(
                    f"    - {fk.column} -> {fk.references_schema}.{fk.references_table}({fk.references_column})"
                )
        return "\n".join(lines)


class DatabaseSchema(BaseModel):
    """Represents complete database schema."""

    database_name: str = Field(..., description="Database identifier (source/target)")
    tables: dict[str, TableInfo] = Field(default_factory=dict, description="Tables by full name")
    extraction_timestamp: str = Field(..., description="When schema was extracted")
    summary: dict[str, Any] = Field(default_factory=dict, description="Summary statistics")

    def get_table(self, schema: str, table: str) -> TableInfo | None:
        """Get table by schema and name."""
        full_name = f"{schema}.{table}"
        return self.tables.get(full_name)

    def get_all_tables(self) -> list[TableInfo]:
        """Get all tables."""
        return list(self.tables.values())

    def to_llm_context(self, max_tables: int | None = None) -> str:
        """Generate context string for LLM consumption."""
        lines = [f"Database: {self.database_name}"]
        lines.append(f"Total tables: {len(self.tables)}")
        lines.append("")

        tables = list(self.tables.values())
        if max_tables and len(tables) > max_tables:
            tables = tables[:max_tables]
            lines.append(f"(Showing first {max_tables} tables)")

        for table in tables:
            lines.append(table.to_ddl_summary())
            lines.append("")

        return "\n".join(lines)


class ColumnDifference(BaseModel):
    """Represents difference between source and target columns."""

    column_name: str
    difference_type: str  # 'missing_in_target', 'missing_in_source', 'type_mismatch', 'nullable_mismatch'
    source_value: str | None = None
    target_value: str | None = None
    description: str


class TableDifference(BaseModel):
    """Represents difference between source and target tables."""

    table_name: str
    difference_type: str  # 'missing_in_target', 'missing_in_source', 'column_differences'
    column_differences: list[ColumnDifference] = Field(default_factory=list)
    description: str


class SchemaComparisonResult(BaseModel):
    """Result of comparing source and target schemas."""

    source_database: str
    target_database: str
    comparison_timestamp: str
    is_compatible: bool
    differences: list[TableDifference] = Field(default_factory=list)
    matching_tables: list[str] = Field(default_factory=list)
    summary: dict[str, Any] = Field(default_factory=dict)
