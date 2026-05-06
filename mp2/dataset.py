from __future__ import annotations

import csv
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Iterator, TextIO

from .s3 import open_s3_text


@dataclass(frozen=True)
class LogRecord:
    timestamp: str
    request_id: str
    user_id: str
    service_name: str
    endpoint: str
    http_method: str
    status_code: int
    response_time_ms: int
    region: str
    error_type: str | None


def _open_input_text(path_or_s3: str) -> TextIO:
    if path_or_s3.startswith("s3://"):
        return open_s3_text(path_or_s3, encoding="utf-8")
    p = Path(path_or_s3)
    return p.open("r", encoding="utf-8-sig", newline="")


def iter_records(path_or_s3: str) -> Iterator[LogRecord]:
    f = _open_input_text(path_or_s3)
    try:
        reader = csv.DictReader(f)
        for row in reader:
            error_type = (row.get("error_type") or "").strip() or None
            yield LogRecord(
                timestamp=(row.get("timestamp") or "").strip(),
                request_id=(row.get("request_id") or "").strip(),
                user_id=(row.get("user_id") or "").strip(),
                service_name=(row.get("service_name") or "").strip(),
                endpoint=(row.get("endpoint") or "").strip(),
                http_method=(row.get("http_method") or "").strip(),
                status_code=int(row.get("status_code") or 0),
                response_time_ms=int(row.get("response_time_ms") or 0),
                region=(row.get("region") or "").strip(),
                error_type=error_type,
            )
    finally:
        f.close()


def chunked(iterable: Iterable[LogRecord], chunk_size: int) -> Iterator[list[LogRecord]]:
    buf: list[LogRecord] = []
    for item in iterable:
        buf.append(item)
        if len(buf) >= chunk_size:
            yield buf
            buf = []
    if buf:
        yield buf
