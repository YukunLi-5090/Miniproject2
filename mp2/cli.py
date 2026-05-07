from __future__ import annotations

import argparse
from pathlib import Path

from .mapreduce_baseline import run_mapreduce_baseline
from .ray_degradation import Thresholds, run_ray_degradation
from .s3 import download_file, upload_file


def _add_common_io(p: argparse.ArgumentParser) -> None:
    p.add_argument("--input", required=True, help="Local CSV path or s3://bucket/key")
    p.add_argument("--outdir", default="outputs", help="Output directory (default: outputs)")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="mp2", description="Mini-Project 2 log analytics tool (S3 / MapReduce / Ray)")
    sub = parser.add_subparsers(dest="cmd", required=True)

    p_upload = sub.add_parser("upload-s3", help="Upload a local dataset file to AWS S3")
    p_upload.add_argument("--input", required=True, help="Local CSV path")
    p_upload.add_argument("--bucket", required=True, help="S3 bucket name")
    p_upload.add_argument("--key", required=True, help="S3 object key (e.g., logs/dataset.csv)")

    p_download = sub.add_parser("download-s3", help="Download an AWS S3 object to a local file")
    p_download.add_argument("--input", required=True, help="s3://bucket/key")
    p_download.add_argument("--output", required=True, help="Local output path (e.g., data/dataset.csv)")

    p_mr = sub.add_parser("mapreduce", help="MapReduce baseline analytics (local key-value aggregation)")
    _add_common_io(p_mr)

    p_ray = sub.add_parser("ray", help="Ray parallel degraded-service detection")
    _add_common_io(p_ray)
    p_ray.add_argument("--chunk-size", type=int, default=5000, help="Chunk size (default: 5000 rows)")
    p_ray.add_argument("--slow-ms", type=int, default=800, help="Slow request threshold in ms (default: 800)")
    p_ray.add_argument("--slow-rate", type=float, default=0.20, help="Slow request rate threshold (default: 0.20)")
    p_ray.add_argument("--server-error-rate", type=float, default=0.10, help="Server error rate threshold (default: 0.10)")
    p_ray.add_argument("--timeout-count", type=int, default=5, help="Timeout error count threshold (default: 5)")
    p_ray.add_argument("--ray-address", default=None, help="Ray cluster address (optional, default: local)")

    args = parser.parse_args(argv)

    if args.cmd == "upload-s3":
        upload_file(args.input, args.bucket, args.key)
        return 0

    if args.cmd == "download-s3":
        download_file(args.input, args.output)
        return 0

    if args.cmd == "mapreduce":
        run_mapreduce_baseline(args.input, Path(args.outdir))
        return 0

    if args.cmd == "ray":
        thresholds = Thresholds(
            slow_ms=args.slow_ms,
            slow_rate=args.slow_rate,
            server_error_rate=args.server_error_rate,
            timeout_count=args.timeout_count,
        )
        run_ray_degradation(
            input_path_or_s3=args.input,
            outdir=Path(args.outdir),
            chunk_size=args.chunk_size,
            thresholds=thresholds,
            ray_address=args.ray_address,
        )
        return 0

    parser.error("Unknown command")
    return 2
