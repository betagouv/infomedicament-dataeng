"""Configuration management via environment variables."""

import os
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urlparse

from dotenv import load_dotenv

load_dotenv(dotenv_path=Path.cwd() / ".env", override=False)


@dataclass
class S3Config:
    """S3/Cellar configuration."""

    endpoint_url: str
    access_key: str
    secret_key: str
    bucket_name: str
    notice_prefix: str  # Prefix for Notices HTML files in bucket
    rcp_prefix: str  # Prefix for RCP HTML files in bucket
    ema_pdf_prefix: str  # Prefix for cached EMA product-information PDFs
    image_prefix: str  # Prefix for CDN-served content images (must match CDN_BASE_URL's path)

    @classmethod
    def from_env(cls) -> "S3Config":
        """Load S3 config from environment variables."""
        return cls(
            endpoint_url=os.environ.get("S3_HOST", "https://cellar-c2.services.clever-cloud.com"),
            access_key=os.environ.get("S3_KEY_ID", ""),
            secret_key=os.environ.get("S3_KEY_SECRET", ""),
            bucket_name=os.environ.get("S3_BUCKET_NAME", "info-medicaments"),
            notice_prefix=os.environ.get("S3_HTML_NOTICE_PREFIX", "imports/notice/"),
            rcp_prefix=os.environ.get("S3_HTML_RCP_PREFIX", "imports/rcp/"),
            ema_pdf_prefix=os.environ.get("S3_EMA_PDF_PREFIX", "imports/ema_pdf/"),
            image_prefix=os.environ.get("S3_IMAGE_PREFIX", "exports/images/"),
        )

    def is_configured(self) -> bool:
        """Check if S3 credentials are configured."""
        return bool(self.access_key and self.secret_key)


@dataclass
class PostgresConfig:
    """PostgreSQL database configuration (for ATC data)."""

    host: str
    user: str
    password: str
    database: str
    port: int

    @classmethod
    def from_env(cls) -> "PostgresConfig":
        """
        Load PostgreSQL config from environment variables.

        Supports two formats:
        1. POSTGRESQL_URL or SCALINGO_POSTGRESQL_URL (e.g., postgres://user:pass@host:port/db)
        2. Individual PG_* environment variables (fallback for local dev)
        """
        database_url = os.environ.get("POSTGRESQL_URL") or os.environ.get("SCALINGO_POSTGRESQL_URL")

        if database_url:
            parsed = urlparse(database_url)
            return cls(
                host=parsed.hostname or "localhost",
                user=parsed.username or "postgres",
                password=parsed.password or "postgres",
                database=parsed.path.lstrip("/") if parsed.path else "postgres",
                port=parsed.port or 5432,
            )

        return cls(
            host=os.environ.get("PG_HOST", "localhost"),
            user=os.environ.get("PG_USER", "postgres"),
            password=os.environ.get("PG_PASSWORD", "postgres"),
            database=os.environ.get("PG_DATABASE", "postgres"),
            port=int(os.environ.get("PG_PORT", "5432")),
        )


@dataclass
class AppConfig:
    """Application configuration."""

    s3: S3Config
    postgres: PostgresConfig
    cdn_base_url: str
    log_level: str

    @classmethod
    def from_env(cls) -> "AppConfig":
        """Load all configuration from environment variables."""
        return cls(
            s3=S3Config.from_env(),
            postgres=PostgresConfig.from_env(),
            cdn_base_url=os.environ.get(
                "CDN_BASE_URL", "https://cellar-c2.services.clever-cloud.com/info-medicaments/exports/images"
            ),
            log_level=os.environ.get("LOG_LEVEL", "INFO"),
        )


# Global config instance (lazy loaded)
_config: AppConfig | None = None


def get_config() -> AppConfig:
    """Get the application configuration (singleton)."""
    global _config
    if _config is None:
        _config = AppConfig.from_env()
    return _config
