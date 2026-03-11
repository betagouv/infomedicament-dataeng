"""S3/Cellar operations for reading and writing files."""

import logging
from typing import Iterator

import boto3
from botocore.config import Config as BotoConfig

from .config import S3Config

logger = logging.getLogger(__name__)


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
                response_checksum_validation="when_required"
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

    def list_parsed_files(self, pattern: str) -> Iterator[str]:
        """
        List parsed JSONL files in the output prefix matching the pattern.

        Args:
            pattern: "N" for Notice files, "R" for RCP files

        Yields:
            Object keys for matching JSONL files
        """
        prefix = self.config.output_prefix
        paginator = self.client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=self.config.bucket_name, Prefix=prefix):
            for obj in page.get("Contents", []):
                key = obj["Key"]
                filename = key.split("/")[-1]
                if filename.startswith(f"parsed_{pattern}_") and filename.endswith(".jsonl"):
                    yield key

    def get_filename_from_key(self, key: str) -> str:
        """Extract the filename from an S3 key."""
        return key.split("/")[-1]
