"""
Application settings and configuration management.
Uses Pydantic Settings for type-safe configuration with environment variable support.
"""

from functools import lru_cache
from typing import Literal

from pydantic import Field, SecretStr, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class DatabaseSettings(BaseSettings):
    """Database connection settings."""

    model_config = SettingsConfigDict(
        env_prefix="",
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # Source Database (PostgreSQL)
    source_db_uri: SecretStr = Field(
        ...,
        description="PostgreSQL connection URI for source database",
        alias="SOURCE_DB_URI",
    )

    # Target Database (PostgreSQL)
    target_db_uri: SecretStr = Field(
        ...,
        description="PostgreSQL connection URI for target database",
        alias="TARGET_DB_URI",
    )

    # Connection pool settings
    db_pool_size: int = Field(default=10, ge=1, le=100, alias="DB_POOL_SIZE")
    db_max_overflow: int = Field(default=20, ge=0, le=100, alias="DB_MAX_OVERFLOW")
    db_pool_timeout: int = Field(default=30, ge=5, le=300, alias="DB_POOL_TIMEOUT")
    db_pool_recycle: int = Field(default=1800, ge=300, le=7200, alias="DB_POOL_RECYCLE")

    # Query execution settings
    query_timeout: int = Field(default=300, ge=30, le=3600, alias="QUERY_TIMEOUT")
    batch_size: int = Field(default=10000, ge=1000, le=100000, alias="BATCH_SIZE")


class LLMSettings(BaseSettings):
    """LLM provider settings."""

    model_config = SettingsConfigDict(
        env_prefix="",
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # LLM Provider
    llm_provider: Literal["openai", "azure_openai", "anthropic", "ollama"] = Field(
        default="openai",
        alias="LLM_PROVIDER",
    )

    # OpenAI settings
    openai_api_key: SecretStr | None = Field(default=None, alias="OPENAI_API_KEY")
    openai_model: str = Field(default="gpt-4o", alias="OPENAI_MODEL")
    openai_base_url: str | None = Field(default=None, alias="OPENAI_BASE_URL")

    # Azure OpenAI settings
    azure_openai_api_key: SecretStr | None = Field(
        default=None, alias="AZURE_OPENAI_API_KEY"
    )
    azure_openai_endpoint: str | None = Field(
        default=None, alias="AZURE_OPENAI_ENDPOINT"
    )
    azure_openai_deployment: str | None = Field(
        default=None, alias="AZURE_OPENAI_DEPLOYMENT"
    )
    azure_openai_api_version: str = Field(
        default="2024-02-15-preview", alias="AZURE_OPENAI_API_VERSION"
    )

    # Anthropic settings
    anthropic_api_key: SecretStr | None = Field(
        default=None, alias="ANTHROPIC_API_KEY"
    )
    anthropic_model: str = Field(default="claude-sonnet-4-20250514", alias="ANTHROPIC_MODEL")

    # Ollama settings
    ollama_base_url: str = Field(
        default="http://localhost:11434", alias="OLLAMA_BASE_URL"
    )
    ollama_model: str = Field(default="llama3.1", alias="OLLAMA_MODEL")

    # LLM parameters
    llm_temperature: float = Field(default=0.0, ge=0.0, le=2.0, alias="LLM_TEMPERATURE")
    llm_max_tokens: int = Field(default=4096, ge=256, le=128000, alias="LLM_MAX_TOKENS")
    llm_timeout: int = Field(default=120, ge=30, le=600, alias="LLM_TIMEOUT")


class AppSettings(BaseSettings):
    """Application-level settings."""

    model_config = SettingsConfigDict(
        env_prefix="",
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # Application settings
    app_name: str = Field(default="ValAgent", alias="APP_NAME")
    app_env: Literal["development", "staging", "production"] = Field(
        default="development", alias="APP_ENV"
    )
    debug: bool = Field(default=False, alias="DEBUG")
    log_level: Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"] = Field(
        default="INFO", alias="LOG_LEVEL"
    )

    # API settings
    api_host: str = Field(default="0.0.0.0", alias="API_HOST")
    api_port: int = Field(default=8000, ge=1, le=65535, alias="API_PORT")
    api_workers: int = Field(default=4, ge=1, le=32, alias="API_WORKERS")
    api_reload: bool = Field(default=False, alias="API_RELOAD")

    # CORS settings
    cors_origins: list[str] = Field(
        default=["http://localhost:3000", "http://localhost:8080"],
        alias="CORS_ORIGINS",
    )

    # Security
    secret_key: SecretStr = Field(
        default=SecretStr("change-me-in-production"),
        alias="SECRET_KEY",
    )
    api_key_header: str = Field(default="X-API-Key", alias="API_KEY_HEADER")

    # Rate limiting
    rate_limit_requests: int = Field(default=100, ge=1, alias="RATE_LIMIT_REQUESTS")
    rate_limit_period: int = Field(default=60, ge=1, alias="RATE_LIMIT_PERIOD")

    # Report settings
    reports_dir: str = Field(default="./reports", alias="REPORTS_DIR")
    max_report_age_days: int = Field(default=30, ge=1, le=365, alias="MAX_REPORT_AGE_DAYS")

    @field_validator("cors_origins", mode="before")
    @classmethod
    def parse_cors_origins(cls, v: str | list[str]) -> list[str]:
        """Parse CORS origins from comma-separated string or list."""
        if isinstance(v, str):
            return [origin.strip() for origin in v.split(",") if origin.strip()]
        return v


class Settings(BaseSettings):
    """Main settings class combining all configuration sections."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    db: DatabaseSettings = Field(default_factory=DatabaseSettings)
    llm: LLMSettings = Field(default_factory=LLMSettings)
    app: AppSettings = Field(default_factory=AppSettings)


@lru_cache
def get_settings() -> Settings:
    """Get cached settings instance."""
    return Settings()
