from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from time import perf_counter

from .common import ensure_dir, runtime_metadata, write_csv, write_json
from .dataset import iter_records


def run_mapreduce_baseline(input_path_or_s3: str, outdir: str | Path) -> dict[str, Path]:
    started = perf_counter()
    outdir_p = ensure_dir(outdir)

    requests_by_service: dict[str, int] = defaultdict(int)
    server_errors_by_service: dict[str, int] = defaultdict(int)
    slow_requests_by_endpoint: dict[tuple[str, str], int] = defaultdict(int)
    slow_ms = 800

    row_count = 0
    for r in iter_records(input_path_or_s3):
        row_count += 1
        requests_by_service[r.service_name] += 1
        if r.status_code >= 500:
            server_errors_by_service[r.service_name] += 1

        if r.response_time_ms > slow_ms:
            slow_requests_by_endpoint[(r.service_name, r.endpoint)] += 1

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

    top10 = sorted(
        slow_requests_by_endpoint.items(),
        key=lambda x: (-x[1], x[0][0], x[0][1]),
    )[:10]
    top10_csv = outdir_p / "top10_slowest_endpoints.csv"
    write_csv(
        top10_csv,
        headers=["rank", "service_name", "endpoint", "slow_request_count"],
        rows=(
            (i + 1, svc, endpoint, cnt) for i, ((svc, endpoint), cnt) in enumerate(top10)
        ),
    )

    meta = runtime_metadata(
        {
            "input": input_path_or_s3,
            "rows": row_count,
            "runtime_seconds": round(perf_counter() - started, 6),
            "execution_environment": "local Python process",
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
