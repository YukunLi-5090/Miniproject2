from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path

from .common import ensure_dir, runtime_metadata, write_csv, write_json
from .dataset import chunked, iter_records, LogRecord


@dataclass(frozen=True)
class Thresholds:
    slow_ms: int = 800
    slow_rate: float = 0.20
    server_error_rate: float = 0.10
    timeout_count: int = 5


def _merge_counts(
    acc: dict[str, list[int]],
    part: dict[str, list[int]],
) -> None:
    for svc, counts in part.items():
        a = acc.setdefault(svc, [0, 0, 0, 0])
        a[0] += counts[0]
        a[1] += counts[1]
        a[2] += counts[2]
        a[3] += counts[3]


def run_ray_degradation(
    input_path_or_s3: str,
    outdir: str | Path,
    chunk_size: int = 5000,
    thresholds: Thresholds | None = None,
    ray_address: str | None = None,
) -> dict[str, Path]:
    import ray

    t = thresholds or Thresholds()
    outdir_p = ensure_dir(outdir)

    ray.init(address=ray_address, ignore_reinit_error=True, include_dashboard=False, log_to_driver=False)

    @ray.remote
    def process_chunk(rows: list[LogRecord], slow_ms: int) -> dict[str, list[int]]:
        out: dict[str, list[int]] = {}
        for r in rows:
            c = out.setdefault(r.service_name, [0, 0, 0, 0])
            c[0] += 1
            if r.response_time_ms > slow_ms:
                c[1] += 1
            if r.status_code >= 500:
                c[2] += 1
            if (r.error_type or "").lower() == "timeout":
                c[3] += 1
        return out

    futures = []
    total_rows = 0
    for ch in chunked(iter_records(input_path_or_s3), chunk_size=chunk_size):
        total_rows += len(ch)
        futures.append(process_chunk.remote(ch, t.slow_ms))

    combined: dict[str, list[int]] = {}
    for part in ray.get(futures):
        _merge_counts(combined, part)

    degraded_rows = []
    degraded_count = 0

    for svc, (total, slow, server_err, timeout_err) in sorted(combined.items(), key=lambda x: x[0]):
        if total <= 0:
            continue
        slow_rate = slow / total
        server_err_rate = server_err / total

        reasons: list[str] = []
        if slow_rate > t.slow_rate:
            reasons.append("高慢请求率")
        if server_err_rate > t.server_error_rate:
            reasons.append("高服务器错误率")
        if timeout_err >= t.timeout_count:
            reasons.append("重复 Timeout 错误")

        if reasons:
            degraded_count += 1
            degraded_rows.append(
                (
                    svc,
                    total,
                    slow,
                    round(slow_rate, 6),
                    server_err,
                    round(server_err_rate, 6),
                    timeout_err,
                    " | ".join(reasons),
                )
            )

    degraded_csv = outdir_p / "degraded_services.csv"
    write_csv(
        degraded_csv,
        headers=[
            "service_name",
            "total_requests",
            "slow_requests",
            "slow_rate",
            "server_errors",
            "server_error_rate",
            "timeout_errors",
            "reasons",
        ],
        rows=degraded_rows,
    )

    degraded_min_csv = outdir_p / "degraded_service_detection.csv"
    write_csv(
        degraded_min_csv,
        headers=["service_name", "reason"],
        rows=((r[0], r[7]) for r in degraded_rows),
    )

    summary = runtime_metadata(
        {
            "input": input_path_or_s3,
            "rows": total_rows,
            "thresholds": {
                "slow_ms": t.slow_ms,
                "slow_rate_gt": t.slow_rate,
                "server_error_rate_gt": t.server_error_rate,
                "timeout_count_gte": t.timeout_count,
            },
            "services_total": len(combined),
            "services_degraded": degraded_count,
        }
    )
    summary_json = outdir_p / "degradation_summary.json"
    write_json(summary_json, summary)

    try:
        ray.shutdown()
    except Exception:
        pass

    return {
        "degraded_service_detection": degraded_min_csv,
        "degraded_services": degraded_csv,
        "degradation_summary": summary_json,
    }
