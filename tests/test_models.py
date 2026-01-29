"""
Tests for schema models.
"""

import pytest
from src.etl_validator.models.schema import (
    ColumnInfo,
    TableInfo,
    DatabaseSchema,
)


class TestColumnInfo:
    """Tests for ColumnInfo model."""

    def test_basic_column(self):
        """Test creating a basic column."""
        col = ColumnInfo(
            name="id",
            position=1,
            data_type="integer",
            nullable=False,
        )
        assert col.name == "id"
        assert col.data_type == "integer"
        assert not col.nullable

    def test_varchar_column_with_length(self):
        """Test varchar column with max length."""
        col = ColumnInfo(
            name="email",
            position=2,
            data_type="character varying",
            max_length=255,
            nullable=False,
        )
        assert col.get_full_type() == "character varying(255)"

    def test_numeric_column_with_precision(self):
        """Test numeric column with precision and scale."""
        col = ColumnInfo(
            name="amount",
            position=3,
            data_type="numeric",
            precision=10,
            scale=2,
            nullable=True,
        )
        assert col.get_full_type() == "numeric(10,2)"


class TestTableInfo:
    """Tests for TableInfo model."""

    def test_table_full_name(self):
        """Test getting full table name."""
        table = TableInfo(
            schema_name="public",
            table_name="customers",
        )
        assert table.full_name == "public.customers"

    def test_table_with_columns(self):
        """Test table with columns."""
        table = TableInfo(
            schema_name="public",
            table_name="customers",
            columns=[
                ColumnInfo(name="id", position=1, data_type="integer"),
                ColumnInfo(name="email", position=2, data_type="varchar"),
            ],
        )
        assert len(table.columns) == 2
        assert table.get_column("id") is not None
        assert table.get_column("nonexistent") is None

    def test_table_column_names(self):
        """Test getting column names."""
        table = TableInfo(
            schema_name="public",
            table_name="customers",
            columns=[
                ColumnInfo(name="id", position=1, data_type="integer"),
                ColumnInfo(name="email", position=2, data_type="varchar"),
            ],
        )
        names = table.get_column_names()
        assert names == ["id", "email"]


class TestDatabaseSchema:
    """Tests for DatabaseSchema model."""

    def test_get_table(self):
        """Test getting table from schema."""
        schema = DatabaseSchema(
            database_name="test",
            tables={
                "public.customers": TableInfo(
                    schema_name="public",
                    table_name="customers",
                ),
            },
            extraction_timestamp="2026-01-30T00:00:00Z",
        )
        
        table = schema.get_table("public", "customers")
        assert table is not None
        assert table.table_name == "customers"

    def test_get_all_tables(self):
        """Test getting all tables."""
        schema = DatabaseSchema(
            database_name="test",
            tables={
                "public.customers": TableInfo(
                    schema_name="public",
                    table_name="customers",
                ),
                "public.orders": TableInfo(
                    schema_name="public",
                    table_name="orders",
                ),
            },
            extraction_timestamp="2026-01-30T00:00:00Z",
        )
        
        tables = schema.get_all_tables()
        assert len(tables) == 2
