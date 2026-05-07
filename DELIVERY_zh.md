## 交付说明（组内）

本仓库用于完成 Mini-Project 2（Cloud Service Log Analytics，从 MapReduce 到 Ray）。

当前代码已经实现：

- AWS S3：上传/下载与直接从 `s3://bucket/key` 读取
- Task 2（MapReduce 基线分析）：3 个输出文件
- Task 3（Ray 扩展分析）：并行降级服务检测（含题目要求的两列输出）

本文件用于说明组员还需要完成的工作、以及如何在本地/云端运行并验证结果一致性。

## 组员仍需完成的工作

1) 安全与账号
- 如果密钥曾在聊天/截图中暴露：立即在 IAM 禁用/删除该 Access Key，并重新生成新的密钥
- 确保不要把密钥写入代码、README 或提交到仓库

2) AWS 侧准备（Task 1 交付材料）
- 创建/确认 S3 bucket（记下 region）
- 上传数据集对象到 bucket（记下 object key，注意包含空格时要加引号）
- 在报告中写清楚：数据集存放位置（bucket/key/region）以及为什么对象存储适合日志数据

3) 运行与结果验证（建议写入报告的“正确性证据”）
- 用 S3 输入跑一遍，把结果输出到 `outputs/`
- 用本地 `data/Comp3041J MiniProject 2 Dataset.csv` 再跑一遍，把结果输出到 `outputs_test/`
- 对比两次输出的 CSV 是否完全一致
  - 预期：CSV 应一致；JSON 中包含运行时间、cwd、input 等字段会不同属于正常

4) 报告写作（项目最终交付）
- Task 2：解释 3 个输出分别代表什么、慢请求判定为 `response_time_ms > 800`
- Task 3：解释 degraded 判定依据（slow rate / server error rate / timeout count），并说明 Ray 如何并行处理并汇总
- 对比 MapReduce 与 Ray 的处理模型（批处理 key-value 聚合 vs 并行任务分块）
- 记录运行环境（本机/云端、Ray local/cluster、Python 版本等）

## 本地运行前准备（Windows / PowerShell）

1) 安装依赖

```powershell
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
```

2) 配置 AWS 凭证（二选一）

方式 A：环境变量（只对当前 PowerShell 窗口有效）

```powershell
$env:AWS_ACCESS_KEY_ID="你的AK"
$env:AWS_SECRET_ACCESS_KEY="你的SK"
$env:AWS_DEFAULT_REGION="us-east-1"
```

方式 B：AWS CLI（长期推荐）

```powershell
aws configure
```

3) 简单验证（建议先做）

```powershell
aws sts get-caller-identity
aws s3 ls "s3://<your-bucket>/<your-object-key>"
```

## 如何从云端（S3）分析

注意：如果 key 含空格，必须用引号包住 `s3://...`。

```powershell
python -m mp2 mapreduce --input "s3://<your-bucket>/Comp3041J MiniProject 2 Dataset.csv" --outdir outputs
python -m mp2 ray --input "s3://<your-bucket>/Comp3041J MiniProject 2 Dataset.csv" --outdir outputs
```

## 如何从本地分析

确保本地数据集存在：

- `data/Comp3041J MiniProject 2 Dataset.csv`

运行：

```powershell
python -m mp2 mapreduce --input "data/Comp3041J MiniProject 2 Dataset.csv" --outdir outputs_test
python -m mp2 ray --input "data/Comp3041J MiniProject 2 Dataset.csv" --outdir outputs_test
```

## 输出文件清单（对照题目）

MapReduce（Task 2）：

- `requests_by_service.csv`
- `server_errors_by_service.csv`（`status_code >= 500`）
- `top10_slowest_endpoints.csv`（慢请求：`response_time_ms > 800`，按慢请求次数排序取 Top10）
- `run_metadata.json`

Ray（Task 3）：

- `degraded_service_detection.csv`（题目要求的最小输出：`service_name,reason`）
- `degraded_services.csv`（详细版指标与原因）
- `degradation_summary.json`

## 如何对比 outputs 与 outputs_test

对比时建议优先看 CSV：

- `requests_by_service.csv`
- `server_errors_by_service.csv`
- `top10_slowest_endpoints.csv`
- `degraded_service_detection.csv`
- `degraded_services.csv`

JSON 文件包含运行元数据字段（时间、cwd、输入路径等）会不同是正常现象。
