from __future__ import annotations

from dataclasses import dataclass
from io import BytesIO, TextIOWrapper
from typing import BinaryIO, TextIO


@dataclass(frozen=True)
class S3Path:
    bucket: str
    key: str

    @staticmethod
    def parse(uri: str) -> "S3Path":
        if not uri.startswith("s3://"):
            raise ValueError("S3 uri must start with s3://")
        rest = uri[len("s3://") :]
        parts = rest.split("/", 1)
        bucket = parts[0].strip()
        if not bucket:
            raise ValueError("Invalid s3 uri bucket")
        key = parts[1] if len(parts) == 2 else ""
        if not key:
            raise ValueError("Invalid s3 uri key")
        return S3Path(bucket=bucket, key=key)


def upload_file(local_path: str, bucket: str, key: str) -> None:
    import boto3

    s3 = boto3.client("s3")
    s3.upload_file(local_path, bucket, key)


def open_s3_text(uri: str, encoding: str = "utf-8") -> TextIO:
    import boto3

    p = S3Path.parse(uri)
    s3 = boto3.client("s3")
    obj = s3.get_object(Bucket=p.bucket, Key=p.key)
    body: BinaryIO = obj["Body"]
    return TextIOWrapper(body, encoding=encoding, newline="")


def download_to_memory(uri: str) -> BytesIO:
    import boto3

    p = S3Path.parse(uri)
    s3 = boto3.client("s3")
    obj = s3.get_object(Bucket=p.bucket, Key=p.key)
    b = obj["Body"].read()
    return BytesIO(b)
