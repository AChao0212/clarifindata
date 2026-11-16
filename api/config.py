"""Environment-driven settings for the collection and publishing subset."""
from functools import lru_cache

from pydantic import AliasChoices, Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    database_url: str = Field(
        default="postgresql://clarifindata:dev@localhost:5432/clarifindata",
        validation_alias=AliasChoices("CFD_DB_URL", "DATABASE_URL"),
    )
    kafka_bootstrap_servers: str = "localhost:9092"
    kafka_topic_raw: str = "raw"
    minio_endpoint: str = "localhost:9000"
    minio_access_key: str = "minioadmin"
    minio_secret_key: str = "minioadmin"
    minio_bucket_raw: str = "raw"
    minio_secure: bool = False
    log_level: str = "INFO"


@lru_cache
def settings() -> Settings:
    return Settings()
