"""Typed runtime configuration loaded from environment variables and ``.env``."""

from __future__ import annotations

from pathlib import Path
from typing import Literal

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

PROJECT_ROOT = Path(__file__).resolve().parent.parent


class Settings(BaseSettings):
    """Runtime settings. Environment variable names match ``.env.example``."""

    model_config = SettingsConfigDict(
        env_file=PROJECT_ROOT / ".env",
        env_file_encoding="utf-8",
        extra="ignore",
        populate_by_name=True,
    )

    lakehouse_storage: Literal["local", "s3"] = Field(
        default="local", validation_alias="LAKEHOUSE_STORAGE"
    )
    lakehouse_local_dir: Path = Field(
        default=PROJECT_ROOT / "data" / "lakehouse", validation_alias="LAKEHOUSE_LOCAL_DIR"
    )
    s3_bucket: str = Field(default="lakehouse", validation_alias="S3_BUCKET")
    s3_prefix: str = Field(default="ecommerce", validation_alias="S3_PREFIX")
    s3_endpoint_url: str | None = Field(default=None, validation_alias="S3_ENDPOINT_URL")
    aws_region: str = Field(default="us-east-1", validation_alias="AWS_REGION")
    aws_access_key_id: str | None = Field(default=None, validation_alias="AWS_ACCESS_KEY_ID")
    aws_secret_access_key: str | None = Field(
        default=None, validation_alias="AWS_SECRET_ACCESS_KEY"
    )
    llm_provider: Literal["mock", "bedrock"] = Field(
        default="mock", validation_alias="LLM_PROVIDER"
    )
    bedrock_model_id: str = Field(
        default="anthropic.claude-3-5-sonnet-20240620-v1:0", validation_alias="BEDROCK_MODEL_ID"
    )
    bedrock_region: str = Field(default="us-east-1", validation_alias="BEDROCK_REGION")
    output_dir: Path = Field(default=PROJECT_ROOT / "output", validation_alias="OUTPUT_DIR")
    semantic_layer_path: Path = Field(
        default=PROJECT_ROOT / "semantic_layer" / "metrics.yaml",
        validation_alias="SEMANTIC_LAYER_PATH",
    )
    sql_row_limit: int = Field(default=1000, ge=1, le=10000, validation_alias="SQL_ROW_LIMIT")
    data_seed: int = Field(default=20260101, validation_alias="DATA_SEED")
    pseudonymization_salt: str | None = Field(
        default=None, validation_alias="PSEUDONYMIZATION_SALT"
    )

    def resolve_path(self, path: Path) -> Path:
        """Resolve relative paths against the project root."""
        return path if path.is_absolute() else PROJECT_ROOT / path

    def table_uri(self, layer: str, name: str) -> str:
        """Return the Delta table URI for a layer/table pair (local path or s3:// URI)."""
        if self.lakehouse_storage == "s3":
            return f"s3://{self.s3_bucket}/{self.s3_prefix}/{layer}/{name}"
        return str(self.resolve_path(self.lakehouse_local_dir) / layer / name)

    def storage_options(self) -> dict[str, str] | None:
        """Options passed to ``deltalake``. ``None`` for local storage."""
        if self.lakehouse_storage == "local":
            return None
        opts: dict[str, str] = {
            "AWS_REGION": self.aws_region,
            # MinIO and some S3-compatible stores need unsafe rename for delta-rs commits.
            "AWS_S3_ALLOW_UNSAFE_RENAME": "true",
        }
        if self.aws_access_key_id and self.aws_secret_access_key:
            opts["AWS_ACCESS_KEY_ID"] = self.aws_access_key_id
            opts["AWS_SECRET_ACCESS_KEY"] = self.aws_secret_access_key
        if self.s3_endpoint_url:
            opts["AWS_ENDPOINT_URL"] = self.s3_endpoint_url
            if self.s3_endpoint_url.startswith("http://"):
                opts["AWS_ALLOW_HTTP"] = "true"
        return opts


def get_settings() -> Settings:
    """Build settings from the current environment."""
    return Settings()
