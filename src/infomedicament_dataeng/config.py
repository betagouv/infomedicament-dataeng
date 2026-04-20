"""Configuration management via environment variables."""

import os
from dataclasses import dataclass
from urllib.parse import urlparse


@dataclass
class S3Config:
    """S3/Cellar configuration."""

    endpoint_url: str
    access_key: str
    secret_key: str
    bucket_name: str
    notice_prefix: str  # Prefix for Notices HTML files in bucket
    rcp_prefix: str  # Prefix for RCP HTML files in bucket
    output_prefix: str  # Prefix for output files (e.g., "exports/parsed/")

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
            output_prefix=os.environ.get("S3_OUTPUT_PREFIX", "exports/parsed/"),
        )

    def is_configured(self) -> bool:
        """Check if S3 credentials are configured."""
        return bool(self.access_key and self.secret_key)


@dataclass
class DatabaseConfig:
    """MySQL database configuration."""

    host: str
    user: str
    password: str
    database: str
    port: int

    @classmethod
    def from_env(cls) -> "DatabaseConfig":
        """
        Load database config from environment variables.

        Supports two formats:
        1. DATABASE_URL or SCALINGO_MYSQL_URL (e.g., mysql://user:pass@host:port/db)
        2. Individual MYSQL_* environment variables (fallback for local dev)
        """
        database_url = os.environ.get("DATABASE_URL") or os.environ.get("SCALINGO_MYSQL_URL")

        if database_url:
            parsed = urlparse(database_url)
            return cls(
                host=parsed.hostname or "localhost",
                user=parsed.username or "root",
                password=parsed.password or "",
                database=parsed.path.lstrip("/") if parsed.path else "pdbm_bdd",
                port=parsed.port or 3306,
            )

        return cls(
            host=os.environ.get("MYSQL_HOST", "localhost"),
            user=os.environ.get("MYSQL_USER", "root"),
            password=os.environ.get("MYSQL_PASSWORD", "mysql"),
            database=os.environ.get("MYSQL_DATABASE", "pdbm_bdd"),
            port=int(os.environ.get("MYSQL_PORT", "3306")),
        )


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
class OpenSearchConfig:
    """OpenSearch configuration."""

    url: str  # Full connection URL, may include user:password (e.g. http://user:pass@host:port)

    @classmethod
    def from_env(cls) -> "OpenSearchConfig":
        """Load OpenSearch config from environment variables.

        Supports two formats:
        1. SCALINGO_OPENSEARCH_URL or OPENSEARCH_URL (e.g. http://user:pass@host:port)
        2. OPENSEARCH_HOST fallback for local dev (default: http://localhost:9200)
        """
        url = (
            os.environ.get("SCALINGO_OPENSEARCH_URL")
            or os.environ.get("OPENSEARCH_URL")
            or os.environ.get("OPENSEARCH_HOST", "http://localhost:9200")
        )
        return cls(url=url)


@dataclass
class AlbertConfig:
    """Albert API configuration for embeddings."""

    api_key: str
    base_url: str
    model: str

    @classmethod
    def from_env(cls) -> "AlbertConfig":
        return cls(
            api_key=os.environ.get("ALBERT_API_KEY", ""),
            base_url=os.environ.get("ALBERT_API_BASE_URL", "https://albert.api.etalab.gouv.fr/v1"),
            model=os.environ.get("ALBERT_EMBEDDING_MODEL", "BAAI/bge-m3"),
        )

    def is_configured(self) -> bool:
        return bool(self.api_key)


@dataclass
class AppConfig:
    """Application configuration."""

    s3: S3Config
    database: DatabaseConfig
    postgres: PostgresConfig
    opensearch: OpenSearchConfig
    albert: AlbertConfig
    cdn_base_url: str
    log_level: str

    @classmethod
    def from_env(cls) -> "AppConfig":
        """Load all configuration from environment variables."""
        return cls(
            s3=S3Config.from_env(),
            database=DatabaseConfig.from_env(),
            postgres=PostgresConfig.from_env(),
            opensearch=OpenSearchConfig.from_env(),
            albert=AlbertConfig.from_env(),
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
