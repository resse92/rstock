# rstock

## 后端服务

默认启动二进制就是后端服务，包含 Axum API 和 Apalis 定时任务：

`cargo run -p rstock`

服务配置通过环境变量提供，不通过启动参数堆配置。常用环境变量：

- `HTTP_BIND`，默认 `0.0.0.0:8080`
- `DAILY_CRON`，默认 `0 30 15 * * *`
- `MINUTE_CRON`，默认 `0 10 15 * * *`
- `LOCAL_STAGING_DIR`，默认 `data/staging`
- `QMT_API_HOST`、`QMT_API_AUTHORIZATION`
- `S3_HOST`、`S3_BUCKET`、`S3_ACCESS_KEY`、`S3_SECRET_KEY`

接口：

- `GET /healthz`
- `POST /api/v1/sync/daily`：同步日 K，空 body 默认今天，或按 JSON 指定 `date` / `start_date` / `end_date`
- `POST /api/v1/sync/minute`：同步 1 分钟线，空 body 默认今天，或按 JSON 指定 `date` / `start_date` / `end_date`

## Tool 入口

以下入口只用于运维、调试或一次性任务；默认运行路径仍然是后端服务。

### 日 K 同步 Tool

`cargo run -p rstock -- sync-daily --start-date 2020-01-01 --end-date 2020-03-27 --chunk-size 20 --fetch-concurrency 1`

日 K 会先写入 `LOCAL_STAGING_DIR` 下的本地 Parquet，校验可读后再上传到 S3。

### 1 分钟线同步 Tool

`cargo run -p rstock -- sync-minute --start-date 2026-05-24 --end-date 2026-05-24 --chunk-size 100 --fetch-concurrency 4`

分钟线同样先落本地 staging，确认 Parquet 可读后再上传到 `curated/minute_bars_1m/`。

### ZIP/CSV 转本地 Parquet Tool

独立工具子项目在 `tools/zip-csv-to-parquet/`，统一处理股票日线、指数日线、分钟线三类离线转换。

股票日线 ZIP/CSV：

`cargo run -p zip-csv-to-parquet -- daily --input-dir /path/to/daily --output-dir /path/to/parquet`

指数日线 CSV：

`cargo run -p zip-csv-to-parquet -- index-daily --input-dir /path/to/index_daily --output-dir /path/to/parquet`

分钟线 ZIP：

`cargo run -p zip-csv-to-parquet -- minute --input-dir /path/to/minute_zips --output-dir /path/to/parquet`
