"""Tests for S3 object listing behavior."""

import pytest

from infomedicament_dataeng.config import S3Config
from infomedicament_dataeng.s3 import S3Client


class FakePaginator:
    def __init__(self, keys):
        self.keys = keys
        self.calls = []

    def paginate(self, **kwargs):
        self.calls.append(kwargs)
        return [{"Contents": [{"Key": key} for key in self.keys]}]


class FakeBotoClient:
    def __init__(self, paginator):
        self.paginator = paginator

    def get_paginator(self, operation):
        assert operation == "list_objects_v2"
        return self.paginator


@pytest.fixture
def s3_config():
    return S3Config(
        endpoint_url="https://s3.example.test",
        access_key="key",
        secret_key="secret",
        bucket_name="bucket",
        notice_prefix="imports/notice/",
        rcp_prefix="imports/rcp/",
        ema_pdf_prefix="imports/ema_pdf/",
        image_prefix="exports/images/",
    )


@pytest.mark.parametrize(
    ("method_name", "prefix", "keys", "expected"),
    [
        (
            "list_html_files",
            "imports/notice/",
            [
                "imports/notice/N0000001.htm",
                "imports/notice/N0000002.html",
                "imports/notice/staging/N0000003.htm",
                "imports/notice/archive/N0000004.htm",
                "imports/notice/readme.txt",
            ],
            ["imports/notice/N0000001.htm", "imports/notice/N0000002.html"],
        ),
        (
            "list_staging_html_files",
            "imports/notice/staging/",
            [
                "imports/notice/staging/N0000003.htm",
                "imports/notice/staging/rejected/N0000004.htm",
            ],
            ["imports/notice/staging/N0000003.htm"],
        ),
    ],
)
def test_html_listings_return_only_direct_objects(s3_config, method_name, prefix, keys, expected):
    paginator = FakePaginator(keys)
    client = S3Client(s3_config)
    client._client = FakeBotoClient(paginator)

    result = list(getattr(client, method_name)("N"))

    assert result == expected
    assert paginator.calls == [{"Bucket": "bucket", "Prefix": prefix, "Delimiter": "/"}]
