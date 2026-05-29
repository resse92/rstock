# rstock

## 后端服务

默认启动二进制就是后端服务，包含 Axum API 和 Apalis 定时任务：

`cargo run -p rstock`

服务配置统一读取项目根目录的 `config.toml`。可先参考 `config.example.toml` 创建本地配置。

接口：

- `GET /healthz`
- `POST /api/v1/sync/daily`：同步日 K，空 body 默认今天，或按 JSON 指定 `date` / `start_date` / `end_date`
- `POST /api/v1/sync/minute`：同步 1 分钟线，空 body 默认今天，或按 JSON 指定 `date` / `start_date` / `end_date`

## Tool 入口

以下入口只用于离线转换或一次性导入；在线同步由 server 的 API 和 cron 任务负责，不再提供独立 CLI。

### ZIP/CSV 转本地 Parquet Tool

独立工具子项目在 `tools/zip-csv-to-parquet/`，统一处理股票日线、指数日线、分钟线三类离线转换。

股票日线 ZIP/CSV：

`cargo run -p zip-csv-to-parquet -- daily --input-dir /path/to/daily --output-dir /path/to/parquet`

指数日线 CSV：

`cargo run -p zip-csv-to-parquet -- index-daily --input-dir /path/to/index_daily --output-dir /path/to/parquet`

分钟线 ZIP：

`cargo run -p zip-csv-to-parquet -- minute --input-dir /path/to/minute_zips --output-dir /path/to/parquet`

分钟线 ZIP/CSV 直传 S3：

`cargo run -p zip-csv-to-parquet -- minute-s3 --input-dir /path/to/minute_zips`

该命令会把 ZIP/CSV 解析为分区 Parquet，先落本地 `config.toml` 里的 `s3.local_staging_dir`，校验后上传到远端 `s3.bucket/curated/minute_bars_1m/`。
