"""Process settings, read from env (and `.env` if present)."""

from __future__ import annotations

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    temporal_address: str = "localhost:7233"
    temporal_namespace: str = "default"
    task_queue: str = "toy-agent"

    redis_url: str = "redis://localhost:6379/0"
    database_url: str = "postgresql://toy:toy@localhost:5433/toy"

    llm_model: str = Field(default="anthropic/claude-sonnet-5-5", alias="LLM_MODEL")
    litellm_api_key: str | None = Field(default=None, alias="LITELLM_API_KEY")
    litellm_base_url: str | None = Field(default=None, alias="LITELLM_BASE_URL")

    billing_url: str = "http://localhost:8402"
    billing_customer: str = "cus_toy"

    s3_endpoint: str = "http://localhost:9000"
    s3_bucket: str = "claim-check"
    s3_access_key: str = "minioadmin"
    s3_secret_key: str = "minioadmin"


def get_settings() -> Settings:
    return Settings()
