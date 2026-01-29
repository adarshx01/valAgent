"""
Configuration management for ETL Validator.

Uses Pydantic Settings for type-safe configuration with environment variable support.
"""

from functools import lru_cache
from typing import Literal
from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import Field, SecretStr


class Settings(BaseSettings):
    """Application settings with environment variable support."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    # Application Settings
    app_name: str = Field(default="ETL Validation Agent", description="Application name")
    app_version: str = Field(default="1.0.0", description="Application version")
    debug: bool = Field(default=False, description="Debug mode")
    log_level: str = Field(default="INFO", description="Logging level")

    # API Settings
    cors_origins: str = Field(
        default="http://localhost:3000,http://localhost:8080,http://localhost:5173",
        description="Comma-separated CORS origins",
    )
    secret_key: SecretStr = Field(
        default="your-secret-key-change-in-production",
        description="Secret key for security",
    )
    api_key_header: str = Field(default="X-API-Key", description="API key header name")
    rate_limit_requests: int = Field(default=100, description="Rate limit requests")
    rate_limit_period: int = Field(default=60, description="Rate limit period in seconds")

    # Database Settings
    source_db_uri: SecretStr = Field(..., description="Source database URI")
    target_db_uri: SecretStr = Field(..., description="Target database URI")

    # Database Pool Settings
    db_pool_size: int = Field(default=20, description="Database connection pool size")
    db_max_overflow: int = Field(default=30, description="Max overflow connections")
    db_pool_timeout: int = Field(default=30, description="Pool timeout in seconds")
    db_pool_recycle: int = Field(default=1800, description="Pool recycle time in seconds")

    # Parallel Processing Settings
    max_parallel_workers: int = Field(default=8, description="Max parallel workers for query execution")
    batch_size: int = Field(default=10000, description="Batch size for large data processing")
    chunk_size: int = Field(default=50000, description="Chunk size for parallel processing")

    # LLM Settings
    llm_provider: Literal["openai", "azure", "anthropic"] = Field(
        default="openai", description="LLM provider"
    )
    openai_api_key: SecretStr = Field(..., description="OpenAI API key")
    openai_model: str = Field(default="gpt-4.1", description="OpenAI model name")
    openai_temperature: float = Field(default=0.1, description="LLM temperature")
    openai_max_tokens: int = Field(default=4096, description="Max tokens for LLM response")

    # Validation Settings
    max_test_cases_per_rule: int = Field(default=10, description="Max test cases per rule")
    query_timeout: int = Field(default=300, description="Query timeout in seconds")
    max_rows_per_query: int = Field(default=100000, description="Max rows per query result")

    @property
    def cors_origins_list(self) -> list[str]:
        """Parse CORS origins into a list."""
        return [origin.strip() for origin in self.cors_origins.split(",")]


@lru_cache
def get_settings() -> Settings:
    """Get cached settings instance."""
    return Settings()


settings = get_settings()
