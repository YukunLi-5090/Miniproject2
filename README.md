# Mini-Project 2: Cloud Service Log Analytics (MapReduce → Ray)

This repository implements a small end-to-end pipeline for analyzing a synthetic cloud service log dataset:

- Store the dataset in AWS S3 (object storage)
- Run MapReduce-style baseline analytics (local key-value aggregation)
- Run Ray-based parallel analytics for degraded-service detection

The CLI supports both local CSV inputs and `s3://bucket/key` inputs.

## Dataset

Each row is a request log record with the following CSV header:

- `timestamp, request_id, user_id, service_name, endpoint, http_method, status_code, response_time_ms, region, error_type`

Slow request definition used by this project:

- Slow request: `response_time_ms > 800`

## Repository Layout

- `mp2/`: Python package
  - `__main__.py`: module entrypoint (`python -m mp2`)
  - `cli.py`: CLI commands (upload/download S3, mapreduce, ray)
  - `dataset.py`: CSV parser and iterators (local file or S3)
  - `s3.py`: S3 helpers (upload, download, streaming read)
  - `mapreduce_baseline.py`: baseline analytics outputs (Task 2)
  - `ray_degradation.py`: Ray parallel degraded-service detection (Task 3)
  - `common.py`: small IO utilities (CSV/JSON writers, runtime metadata)
- `data/`: local datasets (large CSV files should not be committed)
- `outputs/`: outputs for a run (not committed)

## Installation (Windows)

```bash
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
```

Dependencies are listed in `requirements.txt` (Ray and boto3).

## AWS Setup (One-Time)

### 1) Create an S3 bucket

- Choose a region (e.g., `us-east-1`)
- Keep the dataset file as an object in the bucket

### 2) IAM credentials / permissions

Use an IAM user or role that has at least:

- `s3:GetObject` for reading the dataset during analytics
- `s3:PutObject` if you also want to upload via the CLI
- `s3:ListBucket` (optional) for listing/verification with AWS CLI

Do not hardcode credentials in code and do not commit them.

## Configure AWS Credentials (Local Machine)

Choose one approach:

### Option A: Environment variables (PowerShell)

```powershell
$env:AWS_ACCESS_KEY_ID="YOUR_ACCESS_KEY_ID"
$env:AWS_SECRET_ACCESS_KEY="YOUR_SECRET_ACCESS_KEY"
$env:AWS_DEFAULT_REGION="us-east-1"
```

### Option B: AWS CLI profile

```powershell
aws configure
```

## Quick Validation (Recommended)

Verify your credentials and connectivity before running the project:

```powershell
aws sts get-caller-identity
aws s3 ls "s3://<your-bucket>/<your-object-key>"
```

## Run Analytics (Local File)

Put the course dataset under `data/` (example):

- `data/Comp3041J MiniProject 2 Dataset.csv`

Run:

```powershell
python -m mp2 mapreduce --input "data/Comp3041J MiniProject 2 Dataset.csv" --outdir outputs
python -m mp2 ray --input "data/Comp3041J MiniProject 2 Dataset.csv" --outdir outputs
```

## Run Analytics (Directly from S3)

If your S3 object key contains spaces, you must wrap the `s3://...` input in quotes:

```powershell
python -m mp2 mapreduce --input "s3://<your-bucket>/Comp3041J MiniProject 2 Dataset.csv" --outdir outputs
python -m mp2 ray --input "s3://<your-bucket>/Comp3041J MiniProject 2 Dataset.csv" --outdir outputs
```

## Upload / Download Dataset (Optional)

Upload:

```powershell
python -m mp2 upload-s3 --input "data/Comp3041J MiniProject 2 Dataset.csv" --bucket <your-bucket> --key "logs/dataset.csv"
```

Download:

```powershell
python -m mp2 download-s3 --input "s3://<your-bucket>/Comp3041J MiniProject 2 Dataset.csv" --output "data/Comp3041J MiniProject 2 Dataset.csv"
```

## Outputs

All outputs are written under `--outdir` (default: `outputs/`).

### MapReduce baseline (Task 2)

- `requests_by_service.csv`: request count by `service_name`
- `server_errors_by_service.csv`: server error count by `service_name` (`status_code >= 500`)
- `top10_slowest_endpoints.csv`: Top 10 endpoints by slow request count (`response_time_ms > 800`)
- `run_metadata.json`: run metadata

### Ray degraded-service detection (Task 3)

- `degraded_service_detection.csv`: minimal required output (`service_name,reason`)
- `degraded_services.csv`: detailed per-service metrics and reasons
- `degradation_summary.json`: run metadata and thresholds

## Compare S3 vs Local Results

If the S3 object and the local file contain the same dataset, the CSV outputs should match exactly.
JSON outputs include run metadata fields (timestamps, cwd, input path) that will differ.

Recommended approach:

- Run S3 analysis to `outputs/`
- Run local analysis to `outputs_test/`
- Compare CSVs line-by-line
