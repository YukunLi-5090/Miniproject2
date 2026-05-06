from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path

from .common import ensure_dir, runtime_metadata, write_csv, write_json
from .dataset import iter_records


@dataclass(frozen=True)
class EndpointMax:
    endpoint: str
    http_method: str
    max_response_time_ms: int
    service_name: str
    request_id: str
    timestamp: str


def run_mapreduce_baseline(input_path_or_s3: str, outdir: str | Path) -> dict[str, Path]:
    outdir_p = ensure_dir(outdir)

    requests_by_service: dict[str, int] = defaultdict(int)
    server_errors_by_service: dict[str, int] = defaultdict(int)
    endpoint_max: dict[tuple[str, str], EndpointMax] = {}

    row_count = 0
    for r in iter_records(input_path_or_s3):
        row_count += 1
        requests_by_service[r.service_name] += 1
        if r.status_code >= 500:
            server_errors_by_service[r.service_name] += 1

        k = (r.endpoint, r.http_method)
        prev = endpoint_max.get(k)
        if prev is None or r.response_time_ms > prev.max_response_time_ms:
            endpoint_max[k] = EndpointMax(
                endpoint=r.endpoint,
                http_method=r.http_method,
                max_response_time_ms=r.response_time_ms,
                service_name=r.service_name,
                request_id=r.request_id,
                timestamp=r.timestamp,
            )

    requests_csv = outdir_p / "requests_by_service.csv"
    write_csv(
        requests_csv,
        headers=["service_name", "request_count"],
        rows=((svc, cnt) for svc, cnt in sorted(requests_by_service.items(), key=lambda x: (-x[1], x[0]))),
    )

    errors_csv = outdir_p / "server_errors_by_service.csv"
    write_csv(
        errors_csv,
        headers=["service_name", "server_error_count"],
        rows=(
            (svc, cnt)
            for svc, cnt in sorted(server_errors_by_service.items(), key=lambda x: (-x[1], x[0]))
        ),
    )

    top10 = sorted(endpoint_max.values(), key=lambda e: e.max_response_time_ms, reverse=True)[:10]
    top10_csv = outdir_p / "top10_slowest_endpoints.csv"
    write_csv(
        top10_csv,
        headers=[
            "rank",
            "endpoint",
            "http_method",
            "max_response_time_ms",
            "service_name",
            "request_id",
            "timestamp",
        ],
        rows=(
            (i + 1, e.endpoint, e.http_method, e.max_response_time_ms, e.service_name, e.request_id, e.timestamp)
            for i, e in enumerate(top10)
        ),
    )

    meta = runtime_metadata(
        {
            "input": input_path_or_s3,
            "rows": row_count,
            "outputs": [str(requests_csv), str(errors_csv), str(top10_csv)],
        }
    )
    meta_json = outdir_p / "run_metadata.json"
    write_json(meta_json, meta)

    return {
        "requests_by_service": requests_csv,
        "server_errors_by_service": errors_csv,
        "top10_slowest_endpoints": top10_csv,
        "run_metadata": meta_json,
    }
