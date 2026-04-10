"""S3/Cellar operations for reading and writing files."""

import logging
from datetime import date, datetime
from typing import Iterator

import boto3
from botocore.config import Config as BotoConfig

from .config import S3Config, get_config

logger = logging.getLogger(__name__)


def make_s3_client() -> "S3Client":
    """Create an S3Client from environment config, raising if not configured."""
    config = get_config()
    if not config.s3.is_configured():
        raise RuntimeError("S3 credentials not configured. Set S3_KEY_ID and S3_KEY_SECRET.")
    return S3Client(config.s3)


class S3Client:
    """Client for S3-compatible storage (Clever Cloud Cellar)."""

    def __init__(self, config: S3Config):
        self.config = config
        self._client = None

    @property
    def client(self):
        """Lazy-initialize the S3 client."""
        if self._client is None:
            self._client = boto3.client(
                "s3",
                endpoint_url=self.config.endpoint_url,
                aws_access_key_id=self.config.access_key,
                aws_secret_access_key=self.config.secret_key,
                config=BotoConfig(
                    signature_version="s3v4",
                    # CleverCloud S3 implementation does not support recent data integrity features from AWS.
                    # https://github.com/boto/boto3/issues/4392
                    # https://github.com/boto/boto3/issues/4398#issuecomment-2619946229
                    request_checksum_calculation="when_required",
                    response_checksum_validation="when_required",
                ),
            )
        return self._client

    def list_html_files(self, pattern: str) -> Iterator[str]:
        """
        List HTML files in the bucket matching the pattern.

        Args:
            pattern: "N" for Notice files, "R" for RCP files

        Yields:
            Object keys for matching HTML files
        """
        prefix = self.config.notice_prefix if pattern == "N" else self.config.rcp_prefix

        paginator = self.client.get_paginator("list_objects_v2")

        for page in paginator.paginate(Bucket=self.config.bucket_name, Prefix=prefix):
            for obj in page.get("Contents", []):
                key = obj["Key"]
                if key.endswith(".htm") or key.endswith(".html"):
                    yield key

    def download_file_content(self, key: str) -> bytes:
        """
        Download a file's content from S3.

        Args:
            key: The S3 object key

        Returns:
            The file content as bytes
        """
        response = self.client.get_object(Bucket=self.config.bucket_name, Key=key)
        return response["Body"].read()

    def upload_file_content(self, key: str, content: str | bytes, content_type: str = "application/json") -> None:
        """
        Upload content to S3.

        Args:
            key: The S3 object key
            content: The content to upload (string or bytes)
            content_type: The MIME type of the content
        """
        if isinstance(content, str):
            content = content.encode("utf-8")

        self.client.put_object(
            Bucket=self.config.bucket_name,
            Key=key,
            Body=content,
            ContentType=content_type,
        )
        logger.info(f"Uploaded {key} to S3")

    def list_parsed_files(self, pattern: str, since: date | None = None) -> Iterator[str]:
        """
        List parsed JSONL files in the output prefix matching the pattern.

        Args:
            pattern: "N" for Notice files, "R" for RCP files
            since: If provided, only yield files whose filename timestamp is on or after this date.

        Yields:
            Object keys for matching JSONL files
        """
        prefix = self.config.output_prefix
        paginator = self.client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=self.config.bucket_name, Prefix=prefix):
            for obj in page.get("Contents", []):
                key = obj["Key"]
                filename = key.split("/")[-1]
                if not (filename.startswith(f"parsed_{pattern}_") and filename.endswith(".jsonl")):
                    continue
                if since is not None:
                    # filename format: parsed_N_20260318_120000_batch001.jsonl
                    try:
                        file_date = datetime.strptime(filename.split("_")[2], "%Y%m%d").date()
                        if file_date < since:
                            continue
                    except (IndexError, ValueError):
                        pass  # unparseable filename: include it (fail-open)
                yield key

    def list_staging_html_files(self, pattern: str) -> Iterator[str]:
        """
        List HTML files in the staging subdirectory for the given pattern.

        Args:
            pattern: "N" for Notice files, "R" for RCP files

        Yields:
            Object keys for matching HTML files in the staging subdirectory
        """
        html_prefix = self.config.notice_prefix if pattern == "N" else self.config.rcp_prefix
        staging_prefix = f"{html_prefix}staging/"

        paginator = self.client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=self.config.bucket_name, Prefix=staging_prefix):
            for obj in page.get("Contents", []):
                key = obj["Key"]
                if key.endswith(".htm") or key.endswith(".html"):
                    yield key

    def move_file(self, source_key: str, dest_key: str) -> None:
        """
        Move a file within the bucket (copy + delete, as S3 has no native move).

        Args:
            source_key: The source S3 object key
            dest_key: The destination S3 object key
        """
        self.client.copy_object(
            Bucket=self.config.bucket_name,
            CopySource={"Bucket": self.config.bucket_name, "Key": source_key},
            Key=dest_key,
        )
        self.client.delete_object(Bucket=self.config.bucket_name, Key=source_key)
        logger.info(f"Moved {source_key} → {dest_key}")

    def get_filename_from_key(self, key: str) -> str:
        """Extract the filename from an S3 key."""
        return key.split("/")[-1]
