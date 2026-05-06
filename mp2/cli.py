from __future__ import annotations

import argparse
from pathlib import Path

from .mapreduce_baseline import run_mapreduce_baseline
from .ray_degradation import Thresholds, run_ray_degradation
from .s3 import upload_file


def _add_common_io(p: argparse.ArgumentParser) -> None:
    p.add_argument("--input", required=True, help="本地 CSV 路径或 s3://bucket/key")
    p.add_argument("--outdir", default="outputs", help="输出目录（默认 outputs）")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="mp2", description="Mini-Project 2 日志分析工具（S3 / MapReduce / Ray）")
    sub = parser.add_subparsers(dest="cmd", required=True)

    p_upload = sub.add_parser("upload-s3", help="上传本地数据集到 AWS S3")
    p_upload.add_argument("--input", required=True, help="本地 CSV 路径")
    p_upload.add_argument("--bucket", required=True, help="S3 bucket 名称")
    p_upload.add_argument("--key", required=True, help="S3 object key（例如 logs/dataset.csv）")

    p_mr = sub.add_parser("mapreduce", help="MapReduce 基线分析（本地实现 key-value 聚合）")
    _add_common_io(p_mr)

    p_ray = sub.add_parser("ray", help="Ray 并行降级检测")
    _add_common_io(p_ray)
    p_ray.add_argument("--chunk-size", type=int, default=5000, help="分块大小（默认 5000 行）")
    p_ray.add_argument("--slow-ms", type=int, default=800, help="慢请求阈值 ms（默认 800）")
    p_ray.add_argument("--slow-rate", type=float, default=0.20, help="慢请求比例阈值（默认 0.20）")
    p_ray.add_argument("--server-error-rate", type=float, default=0.10, help="服务器错误率阈值（默认 0.10）")
    p_ray.add_argument("--timeout-count", type=int, default=5, help="Timeout 次数阈值（默认 5）")
    p_ray.add_argument("--ray-address", default=None, help="Ray 集群地址（可选，默认本地）")

    args = parser.parse_args(argv)

    if args.cmd == "upload-s3":
        upload_file(args.input, args.bucket, args.key)
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
