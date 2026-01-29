"""
Test fixtures and configuration for pytest.
"""

import asyncio
from typing import AsyncGenerator, Generator
import pytest
import pytest_asyncio


@pytest.fixture(scope="session")
def event_loop() -> Generator[asyncio.AbstractEventLoop, None, None]:
    """Create an event loop for async tests."""
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()


@pytest.fixture
def sample_business_rules() -> str:
    """Sample business rules for testing."""
    return """
    1. All customer records from source should exist in target.
    2. Email addresses should be stored in lowercase in target.
    3. Order totals should match between source and target.
    4. No NULL values allowed in customer name fields.
    """


@pytest.fixture
def sample_schema_info() -> dict:
    """Sample schema information for testing."""
    return {
        "database": "test",
        "tables": {
            "public.customers": {
                "schema": "public",
                "name": "customers",
                "type": "BASE TABLE",
                "columns": [
                    {
                        "name": "id",
                        "position": 1,
                        "data_type": "integer",
                        "nullable": False,
                    },
                    {
                        "name": "email",
                        "position": 2,
                        "data_type": "varchar",
                        "max_length": 255,
                        "nullable": False,
                    },
                    {
                        "name": "name",
                        "position": 3,
                        "data_type": "varchar",
                        "max_length": 100,
                        "nullable": True,
                    },
                ],
                "primary_keys": ["id"],
                "foreign_keys": [],
                "indexes": [],
                "approximate_row_count": 10000,
            },
            "public.orders": {
                "schema": "public",
                "name": "orders",
                "type": "BASE TABLE",
                "columns": [
                    {
                        "name": "id",
                        "position": 1,
                        "data_type": "integer",
                        "nullable": False,
                    },
                    {
                        "name": "customer_id",
                        "position": 2,
                        "data_type": "integer",
                        "nullable": False,
                    },
                    {
                        "name": "total",
                        "position": 3,
                        "data_type": "numeric",
                        "precision": 10,
                        "scale": 2,
                        "nullable": False,
                    },
                ],
                "primary_keys": ["id"],
                "foreign_keys": [
                    {
                        "column": "customer_id",
                        "references_schema": "public",
                        "references_table": "customers",
                        "references_column": "id",
                        "constraint_name": "fk_customer",
                    }
                ],
                "indexes": [],
                "approximate_row_count": 50000,
            },
        },
        "summary": {
            "total_tables": 2,
            "total_columns": 6,
        },
    }
