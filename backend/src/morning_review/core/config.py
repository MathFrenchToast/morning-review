from __future__ import annotations

import os
from pathlib import Path
from uuid import UUID

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

PROJECT_ROOT = Path(__file__).resolve().parents[4]


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=str(PROJECT_ROOT / ".env"),
        env_file_encoding="utf-8",
        extra="ignore",
    )

    app_env: str = Field(default="development", alias="APP_ENV")
    app_secret_key: str = Field(
        default="dev-secret-key-change-in-production-32bytes",
        alias="APP_SECRET_KEY",
    )

    # Mode MVP (single_user) vs Post-MVP (oidc)
    auth_mode: str = Field(default="single_user", alias="AUTH_MODE")
    bootstrap_user_id: UUID = Field(
        default=UUID("019326b0-7c00-7000-8000-000000000001"),
        alias="BOOTSTRAP_USER_ID",
    )
    bootstrap_user_email: str = Field(default="", alias="BOOTSTRAP_USER_EMAIL")
    mvp_api_token: str = Field(default="", alias="MVP_API_TOKEN")

    # PostgreSQL
    database_url: str = Field(
        default="postgresql+psycopg://morning:morning@localhost:5432/morning_review",
        alias="DATABASE_URL",
    )

    # Stockage S3 compatible (SeaweedFS / MinIO / Garage / AWS S3)
    aws_endpoint_url_s3: str = Field(
        default="http://localhost:9000",
        alias="AWS_ENDPOINT_URL_S3",
    )
    aws_access_key_id: str = Field(default="minioadmin", alias="AWS_ACCESS_KEY_ID")
    aws_secret_access_key: str = Field(default="minioadmin", alias="AWS_SECRET_ACCESS_KEY")
    aws_region: str = Field(default="us-east-1", alias="AWS_REGION")
    s3_bucket_name: str = Field(default="morning-review", alias="S3_BUCKET_NAME")
    s3_addressing_style: str = Field(default="path", alias="S3_ADDRESSING_STYLE")

    # Embeddings ONNX CPU
    embedding_model_name: str = Field(
        default="sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2",
        alias="EMBEDDING_MODEL_NAME",
    )
    embedding_dimensions: int = Field(default=384, alias="EMBEDDING_DIMENSIONS")
    fastembed_cache_path: str = Field(
        default=str(PROJECT_ROOT / ".cache" / "fastembed"),
        alias="FASTEMBED_CACHE_PATH",
    )
    omp_num_threads: int = Field(default=2, alias="OMP_NUM_THREADS")
    topic_similarity_threshold: float = Field(
        default=0.82,
        alias="TOPIC_SIMILARITY_THRESHOLD",
    )
    topic_window_days: int = Field(default=14, alias="TOPIC_WINDOW_DAYS")

    # OpenRouter (:free)
    openrouter_api_key: str = Field(default="", alias="OPENROUTER_API_KEY")
    openrouter_base_url: str = Field(
        default="https://openrouter.ai/api/v1",
        alias="OPENROUTER_BASE_URL",
    )
    openrouter_primary_model: str = Field(
        default="openrouter/free",
        alias="OPENROUTER_PRIMARY_MODEL",
    )
    openrouter_max_rpm: int = Field(default=15, alias="OPENROUTER_MAX_RPM")
    openrouter_max_rpd: int = Field(default=900, alias="OPENROUTER_MAX_RPD")

    # Gmail IMAP (.env)
    gmail_account: str = Field(default="", alias="GMAIL_ACCOUNT")
    gmail_pwd: str = Field(default="", alias="GMAIL_PWD")
    gmail_imap_host: str = Field(default="imap.gmail.com", alias="GMAIL_IMAP_HOST")
    gmail_imap_port: int = Field(default=993, alias="GMAIL_IMAP_PORT")
    gmail_curation_label: str = Field(default="INBOX", alias="GMAIL_CURATION_LABEL")

    # Fichier déclaratif des sources
    sources_yaml_path: Path = Field(
        default=PROJECT_ROOT / "config" / "sources.yaml",
    )

    @property
    def effective_bootstrap_email(self) -> str:
        return self.bootstrap_user_email or self.gmail_account or "admin@localhost"


settings = Settings()
os.environ.setdefault("OMP_NUM_THREADS", str(settings.omp_num_threads))
