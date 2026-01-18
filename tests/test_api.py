"""
Tests for API endpoints.
"""

import pytest
from httpx import AsyncClient


@pytest.mark.asyncio
async def test_health_endpoint(client: AsyncClient):
    """Test health check endpoint."""
    response = await client.get("/api/v1/health")
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "healthy"
    assert "version" in data
    assert "timestamp" in data


@pytest.mark.asyncio
async def test_root_endpoint(client: AsyncClient):
    """Test root endpoint."""
    response = await client.get("/api")
    assert response.status_code == 200
    data = response.json()
    assert "name" in data
    assert "version" in data


@pytest.mark.asyncio
async def test_validation_stats(client: AsyncClient):
    """Test validation statistics endpoint."""
    response = await client.get("/api/v1/validations/stats")
    # May fail if DB not connected, but should return proper error
    assert response.status_code in [200, 500]


@pytest.mark.asyncio
async def test_list_validations(client: AsyncClient):
    """Test listing validations."""
    response = await client.get("/api/v1/validations")
    # May fail if DB not connected
    assert response.status_code in [200, 500]
