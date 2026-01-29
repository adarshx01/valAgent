"""
API Dependencies for FastAPI.

Provides dependency injection for routes.
"""

from typing import Annotated
from fastapi import Depends, Header, HTTPException, status

from ..core.config import settings
from ..agents.validation_agent import ValidationAgent, validation_agent
from ..utils.logger import get_logger

logger = get_logger(__name__)


async def get_validation_agent() -> ValidationAgent:
    """
    Dependency to get the validation agent instance.
    
    Ensures the agent is initialized before use.
    """
    if not validation_agent._initialized:
        await validation_agent.initialize()
    return validation_agent


async def verify_api_key(
    x_api_key: Annotated[str | None, Header()] = None,
) -> str | None:
    """
    Optional API key verification.
    
    If API key is configured, validates the provided key.
    """
    # Skip if no API key configured
    if not settings.secret_key:
        return None

    if not x_api_key:
        return None  # Allow unauthenticated access for now

    # In production, implement proper API key validation
    return x_api_key


class RateLimiter:
    """Simple in-memory rate limiter."""

    def __init__(self, requests: int, period: int):
        self.requests = requests
        self.period = period
        self._request_counts: dict[str, list[float]] = {}

    async def check_rate_limit(self, client_id: str) -> bool:
        """Check if client has exceeded rate limit."""
        import time

        current_time = time.time()
        if client_id not in self._request_counts:
            self._request_counts[client_id] = []

        # Clean old requests
        self._request_counts[client_id] = [
            t for t in self._request_counts[client_id]
            if current_time - t < self.period
        ]

        # Check limit
        if len(self._request_counts[client_id]) >= self.requests:
            return False

        # Add current request
        self._request_counts[client_id].append(current_time)
        return True


rate_limiter = RateLimiter(
    requests=settings.rate_limit_requests,
    period=settings.rate_limit_period,
)


async def check_rate_limit(
    x_forwarded_for: Annotated[str | None, Header()] = None,
) -> None:
    """
    Rate limiting dependency.
    
    Limits requests per client based on configuration.
    """
    client_id = x_forwarded_for or "default"

    if not await rate_limiter.check_rate_limit(client_id):
        raise HTTPException(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            detail={
                "error_code": "RATE_LIMIT_EXCEEDED",
                "message": f"Rate limit exceeded. Max {settings.rate_limit_requests} requests per {settings.rate_limit_period} seconds.",
            },
        )


# Type aliases for common dependencies
ValidationAgentDep = Annotated[ValidationAgent, Depends(get_validation_agent)]
RateLimitDep = Annotated[None, Depends(check_rate_limit)]
