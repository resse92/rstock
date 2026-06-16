pub mod api;
pub mod kline_frame;
pub mod models;
// Tick 标准化仍保留在独立模块；K 线兼容层已移除，主链路统一走 polars DataFrame。
pub mod normalize;
pub mod patterns;
pub mod sync_daily;
pub mod sync_minute;
pub mod tdx_source;
pub mod utils;
