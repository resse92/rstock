# rstock

## 运行

### 抓取日K

`cargo run -p rstock -- sync-daily --start-date 2020-01-01 --end-date 2020-03-27 --chunk-size 20 --fetch-concurrency 1`

### ZIP/CSV 转本地 Parquet

独立工具子项目在 `tools/zip-csv-to-parquet/`，统一处理股票日线、指数日线、分钟线三类离线转换。

股票日线 ZIP/CSV：

`cargo run -p zip-csv-to-parquet -- daily --input-dir /path/to/daily --output-dir /path/to/parquet`

指数日线 CSV：

`cargo run -p zip-csv-to-parquet -- index-daily --input-dir /path/to/index_daily --output-dir /path/to/parquet`

分钟线 ZIP：

`cargo run -p zip-csv-to-parquet -- minute --input-dir /path/to/minute_zips --output-dir /path/to/parquet`
