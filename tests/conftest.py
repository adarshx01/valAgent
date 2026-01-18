"""
Test configuration and fixtures.
"""

import pytest
from httpx import AsyncClient, ASGITransport

from valagent.api.app import create_app


@pytest.fixture
def app():
    """Create application for testing."""
    return create_app()


@pytest.fixture
async def client(app):
    """Create async HTTP client for testing."""
    async with AsyncClient(
        transport=ASGITransport(app=app),
        base_url="http://test"
    ) as ac:
        yield ac


@pytest.fixture
def sample_business_rules():
    """Sample business rules for testing."""
    return [
        "All customer records from source should exist in target",
        "Total order count should match between databases",
        "No duplicate customer IDs in target",
    ]


@pytest.fixture
def sample_schema():
    """Sample database schema for testing."""
    return {
        "tables": {
            "customers": {
                "columns": [
                    {"name": "id", "type": "INTEGER", "primary_key": True, "nullable": False},
                    {"name": "name", "type": "VARCHAR(255)", "nullable": False},
                    {"name": "email", "type": "VARCHAR(255)", "nullable": True},
                    {"name": "created_at", "type": "TIMESTAMP", "nullable": False},
                ],
                "primary_keys": ["id"],
                "foreign_keys": [],
                "indexes": [],
            },
            "orders": {
                "columns": [
                    {"name": "id", "type": "INTEGER", "primary_key": True, "nullable": False},
                    {"name": "customer_id", "type": "INTEGER", "nullable": False},
                    {"name": "total", "type": "DECIMAL(10,2)", "nullable": False},
                    {"name": "status", "type": "VARCHAR(50)", "nullable": False},
                ],
                "primary_keys": ["id"],
                "foreign_keys": [
                    {
                        "columns": ["customer_id"],
                        "referred_table": "customers",
                        "referred_columns": ["id"],
                    }
                ],
                "indexes": [],
            },
        },
        "views": [],
        "schemas": ["public"],
    }
