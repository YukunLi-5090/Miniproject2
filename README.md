# 云服务版 Mini-Project 2

本项目用于分析合成的云服务日志数据集，覆盖：

- AWS S3：数据集上传到对象存储
- MapReduce：批量 key-value 聚合基线分析
- Ray：并行分块分析，输出服务降级报告

## 数据集格式

CSV 表头字段（与课程提供数据一致）：

- timestamp, request_id, user_id, service_name, endpoint, http_method, status_code, response_time_ms, region, error_type

## 目录结构

- mp2/：核心代码（CLI、S3、MapReduce、Ray）
- data/：放置本地数据集（不提交大文件）
- outputs/：分析结果输出（CSV/JSON，不提交生成文件）

## 安装依赖

建议使用虚拟环境：

```bash
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
```

## 快速开始（本地 CSV）

把课程给的 `Comp3041J MiniProject 2 Dataset.csv` 放到 `data/dataset.csv`，然后：

```bash
python -m mp2 mapreduce --input data/dataset.csv --outdir outputs
python -m mp2 ray --input data/dataset.csv --outdir outputs
```

## S3 上传

需要本机已配置 AWS 凭证（例如 `AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY` 或 AWS CLI profile）。

```bash
python -m mp2 upload-s3 --input data/dataset.csv --bucket <your-bucket> --key logs/dataset.csv
```

## 输出说明

MapReduce（outputs/ 下）：

- requests_by_service.csv
- server_errors_by_service.csv
- top10_slowest_endpoints.csv
- run_metadata.json

Ray（outputs/ 下）：

- degraded_services.csv
- degradation_summary.json
