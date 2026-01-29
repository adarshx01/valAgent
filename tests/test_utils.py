"""
Tests for utility functions.
"""

import pytest
from src.etl_validator.utils.helpers import (
    generate_uuid,
    generate_short_id,
    truncate_string,
    compare_values,
    format_row_count,
    parse_table_reference,
    format_duration,
    chunk_list,
    extract_table_names_from_sql,
)


class TestGenerators:
    """Tests for ID generators."""

    def test_generate_uuid(self):
        """Test UUID generation."""
        uuid1 = generate_uuid()
        uuid2 = generate_uuid()
        assert len(uuid1) == 36
        assert uuid1 != uuid2

    def test_generate_short_id(self):
        """Test short ID generation."""
        short_id = generate_short_id()
        assert len(short_id) == 12


class TestStringUtils:
    """Tests for string utilities."""

    def test_truncate_string_short(self):
        """Test truncating short string."""
        result = truncate_string("hello", 10)
        assert result == "hello"

    def test_truncate_string_long(self):
        """Test truncating long string."""
        result = truncate_string("hello world", 8)
        assert result == "hello..."
        assert len(result) == 8


class TestCompareValues:
    """Tests for value comparison."""

    def test_compare_none_values(self):
        """Test comparing None values."""
        assert compare_values(None, None) is True
        assert compare_values(None, 1) is False
        assert compare_values(1, None) is False

    def test_compare_exact_values(self):
        """Test exact value comparison."""
        assert compare_values(100, 100) is True
        assert compare_values(100, 101) is False

    def test_compare_with_tolerance(self):
        """Test comparison with tolerance."""
        assert compare_values(100, 101, tolerance=0.02) is True
        assert compare_values(100, 110, tolerance=0.05) is False

    def test_compare_strings(self):
        """Test string comparison."""
        assert compare_values("hello", "HELLO") is True
        assert compare_values("hello ", "hello") is True


class TestFormatters:
    """Tests for formatters."""

    def test_format_row_count(self):
        """Test row count formatting."""
        assert format_row_count(500) == "500"
        assert format_row_count(1500) == "1.50K"
        assert format_row_count(1500000) == "1.50M"
        assert format_row_count(1500000000) == "1.50B"

    def test_format_duration(self):
        """Test duration formatting."""
        assert format_duration(0.5) == "500ms"
        assert format_duration(5) == "5.00s"
        assert format_duration(65) == "1m 5s"
        assert format_duration(3700) == "1h 1m"


class TestParsers:
    """Tests for parsers."""

    def test_parse_table_reference(self):
        """Test table reference parsing."""
        assert parse_table_reference("public.customers") == ("public", "customers")
        assert parse_table_reference("customers") == ("public", "customers")

    def test_extract_table_names(self):
        """Test extracting table names from SQL."""
        sql = "SELECT * FROM public.customers JOIN orders ON customers.id = orders.customer_id"
        tables = extract_table_names_from_sql(sql)
        assert "public.customers" in tables
        assert "orders" in tables


class TestChunkList:
    """Tests for chunk_list function."""

    def test_chunk_list_even(self):
        """Test chunking list evenly."""
        result = chunk_list([1, 2, 3, 4], 2)
        assert result == [[1, 2], [3, 4]]

    def test_chunk_list_uneven(self):
        """Test chunking list with remainder."""
        result = chunk_list([1, 2, 3, 4, 5], 2)
        assert result == [[1, 2], [3, 4], [5]]

    def test_chunk_list_empty(self):
        """Test chunking empty list."""
        result = chunk_list([], 2)
        assert result == []
