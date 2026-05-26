use std::env;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::Arc;

use anyhow::{Context, Result};
use apalis::prelude::*;
use apalis_cron::{CronContext, CronStream, Schedule};
use axum::extract::State;
use axum::http::StatusCode;
use axum::routing::{get, post};
use axum::{Json, Router};
use chrono::{Local, Utc};
use serde::{Deserialize, Serialize};
use tokio::net::TcpListener;
use tokio::sync::Mutex;

use crate::models::DEFAULT_QMT_API_HOST;
use crate::sync_daily::{run_sync_daily, SyncDailyArgs};
use crate::sync_minute::{run_sync_minute, SyncMinuteArgs};

#[derive(Debug, Clone)]
pub struct ServerConfig {
    pub bind: SocketAddr,
    pub daily_cron: String,
    pub minute_cron: String,
    pub daily_chunk_size: usize,
    pub minute_chunk_size: usize,
    pub daily_fetch_concurrency: usize,
    pub minute_fetch_concurrency: usize,
    pub stock_codes_file: Option<PathBuf>,
    pub base_url: String,
    pub authorization: Option<String>,
    pub timeout: u64,
    pub s3_bucket: String,
    pub staging_dir: PathBuf,
    pub s3_region: String,
    pub s3_access_key: Option<String>,
    pub s3_secret_key: Option<String>,
    pub s3_host: Option<String>,
}

impl ServerConfig {
    pub fn from_env() -> Result<Self> {
        Ok(Self {
            bind: env_var("HTTP_BIND")
                .unwrap_or_else(|| "0.0.0.0:8080".to_string())
                .parse()
                .context("HTTP_BIND 格式错误")?,
            daily_cron: env_var("DAILY_CRON").unwrap_or_else(|| "0 30 15 * * *".to_string()),
            minute_cron: env_var("MINUTE_CRON").unwrap_or_else(|| "0 10 15 * * *".to_string()),
            daily_chunk_size: env_usize("DAILY_CHUNK_SIZE", 200)?,
            minute_chunk_size: env_usize("MINUTE_CHUNK_SIZE", 100)?,
            daily_fetch_concurrency: env_usize("DAILY_FETCH_CONCURRENCY", 8)?,
            minute_fetch_concurrency: env_usize("MINUTE_FETCH_CONCURRENCY", 4)?,
            stock_codes_file: env_var("STOCK_CODES_FILE").map(PathBuf::from),
            base_url: env_var("QMT_API_HOST").unwrap_or_else(|| DEFAULT_QMT_API_HOST.to_string()),
            authorization: env_var("QMT_API_AUTHORIZATION"),
            timeout: env_u64("QMT_API_TIMEOUT", 30)?,
            s3_bucket: env_var("S3_BUCKET").unwrap_or_else(|| "stock".to_string()),
            staging_dir: PathBuf::from(
                env_var("LOCAL_STAGING_DIR").unwrap_or_else(|| "data/staging".to_string()),
            ),
            s3_region: env_var("S3_REGION").unwrap_or_else(|| "us-east-1".to_string()),
            s3_access_key: env_var("S3_ACCESS_KEY"),
            s3_secret_key: env_var("S3_SECRET_KEY"),
            s3_host: env_var("S3_HOST").or_else(|| env_var("s3_host")),
        })
    }
}

#[derive(Clone)]
struct AppState {
    args: Arc<ServerConfig>,
    sync_lock: Arc<Mutex<()>>,
}

#[derive(Debug, Deserialize)]
struct SyncRequest {
    date: Option<String>,
    start_date: Option<String>,
    end_date: Option<String>,
}

#[derive(Debug, Serialize)]
struct ApiResponse {
    ok: bool,
    message: String,
}

#[derive(Default, Debug, Clone)]
struct DailyCron;

#[derive(Default, Debug, Clone)]
struct MinuteCron;

pub async fn run_server(args: ServerConfig) -> Result<()> {
    let state = AppState {
        args: Arc::new(args),
        sync_lock: Arc::new(Mutex::new(())),
    };

    spawn_cron_workers(state.clone())?;

    let app = Router::new()
        .route("/", get(root))
        .route("/healthz", get(healthz))
        .route("/api/v1/sync/daily", post(sync_daily_range))
        .route("/api/v1/sync/minute", post(sync_minute_range))
        .with_state(state.clone());

    let listener = TcpListener::bind(state.args.bind)
        .await
        .with_context(|| format!("bind {} failed", state.args.bind))?;
    println!("[HTTP] listening on {}", state.args.bind);
    axum::serve(listener, app).await?;
    Ok(())
}

fn spawn_cron_workers(state: AppState) -> Result<()> {
    let daily_schedule = Schedule::from_str(&state.args.daily_cron)
        .with_context(|| format!("invalid DAILY_CRON: {}", state.args.daily_cron))?;
    let minute_schedule = Schedule::from_str(&state.args.minute_cron)
        .with_context(|| format!("invalid MINUTE_CRON: {}", state.args.minute_cron))?;

    let daily_worker = WorkerBuilder::new("daily-kline-cron")
        .data(state.clone())
        .backend(CronStream::new(daily_schedule))
        .build_fn(handle_daily_cron);
    let minute_worker = WorkerBuilder::new("minute-kline-cron")
        .data(state)
        .backend(CronStream::new(minute_schedule))
        .build_fn(handle_minute_cron);

    tokio::spawn(async move { daily_worker.run().await });
    tokio::spawn(async move { minute_worker.run().await });
    Ok(())
}

async fn handle_daily_cron(_: DailyCron, _: CronContext<Utc>, data: Data<AppState>) {
    let date = today();
    if let Err(err) = sync_daily_range_inner((*data).clone(), date.clone(), date).await {
        eprintln!("[CRON][daily] {err:#}");
    }
}

async fn handle_minute_cron(_: MinuteCron, _: CronContext<Utc>, data: Data<AppState>) {
    if let Err(err) = sync_minute_for_date((*data).clone(), today()).await {
        eprintln!("[CRON][minute] {err:#}");
    }
}

async fn healthz() -> Json<ApiResponse> {
    Json(ApiResponse {
        ok: true,
        message: "ok".to_string(),
    })
}

async fn root() -> Json<ApiResponse> {
    Json(ApiResponse {
        ok: true,
        message: "rstock service".to_string(),
    })
}

async fn sync_daily_range(
    State(state): State<AppState>,
    req: Option<Json<SyncRequest>>,
) -> Result<Json<ApiResponse>, ApiError> {
    let (start, end) = request_dates(req.map(|Json(req)| req));
    sync_daily_range_inner(state, start, end).await?;
    Ok(Json(ok("daily synced")))
}

async fn sync_minute_range(
    State(state): State<AppState>,
    req: Option<Json<SyncRequest>>,
) -> Result<Json<ApiResponse>, ApiError> {
    let (start, end) = request_dates(req.map(|Json(req)| req));
    sync_minute_range_inner(state, start, end).await?;
    Ok(Json(ok("minute synced")))
}

async fn sync_daily_range_inner(
    state: AppState,
    start_date: String,
    end_date: String,
) -> Result<()> {
    let _guard = state.sync_lock.lock().await;
    run_sync_daily(build_daily_args(&state.args, start_date, end_date)).await
}

async fn sync_minute_for_date(state: AppState, date: String) -> Result<()> {
    sync_minute_range_inner(state, date.clone(), date).await
}

async fn sync_minute_range_inner(
    state: AppState,
    start_date: String,
    end_date: String,
) -> Result<()> {
    let _guard = state.sync_lock.lock().await;
    run_sync_minute(build_minute_args(&state.args, start_date, end_date)).await
}

fn build_daily_args(args: &ServerConfig, start_date: String, end_date: String) -> SyncDailyArgs {
    SyncDailyArgs {
        start_date,
        end_date,
        chunk_size: args.daily_chunk_size,
        fetch_concurrency: args.daily_fetch_concurrency,
        incremental: false,
        watermark_file: PathBuf::from("meta/ingestion/daily_watermark.txt"),
        stock_codes_file: args.stock_codes_file.clone(),
        base_url: args.base_url.clone(),
        authorization: args.authorization.clone(),
        timeout: args.timeout,
        s3_bucket: args.s3_bucket.clone(),
        staging_dir: args.staging_dir.clone(),
        s3_region: args.s3_region.clone(),
        s3_access_key: args.s3_access_key.clone(),
        s3_secret_key: args.s3_secret_key.clone(),
        s3_host: args.s3_host.clone(),
    }
}

fn build_minute_args(args: &ServerConfig, start_date: String, end_date: String) -> SyncMinuteArgs {
    SyncMinuteArgs {
        start_date,
        end_date,
        chunk_size: args.minute_chunk_size,
        fetch_concurrency: args.minute_fetch_concurrency,
        stock_codes_file: args.stock_codes_file.clone(),
        base_url: args.base_url.clone(),
        authorization: args.authorization.clone(),
        timeout: args.timeout,
        s3_bucket: args.s3_bucket.clone(),
        staging_dir: args.staging_dir.clone(),
        s3_region: args.s3_region.clone(),
        s3_access_key: args.s3_access_key.clone(),
        s3_secret_key: args.s3_secret_key.clone(),
        s3_host: args.s3_host.clone(),
    }
}

fn today() -> String {
    Local::now().format("%Y%m%d").to_string()
}

fn request_dates(req: Option<SyncRequest>) -> (String, String) {
    let Some(req) = req else {
        let date = today();
        return (date.clone(), date);
    };
    let start = req
        .start_date
        .or_else(|| req.date.clone())
        .unwrap_or_else(today);
    let end = req.end_date.or(req.date).unwrap_or_else(|| start.clone());
    (start, end)
}

fn ok(message: &str) -> ApiResponse {
    ApiResponse {
        ok: true,
        message: message.to_string(),
    }
}

fn env_var(key: &str) -> Option<String> {
    env::var(key).ok().filter(|v| !v.trim().is_empty())
}

fn env_usize(key: &str, default: usize) -> Result<usize> {
    match env_var(key) {
        Some(value) => value
            .parse::<usize>()
            .with_context(|| format!("{key} 必须是正整数")),
        None => Ok(default),
    }
}

fn env_u64(key: &str, default: u64) -> Result<u64> {
    match env_var(key) {
        Some(value) => value
            .parse::<u64>()
            .with_context(|| format!("{key} 必须是正整数")),
        None => Ok(default),
    }
}

struct ApiError(anyhow::Error);

impl<E> From<E> for ApiError
where
    E: Into<anyhow::Error>,
{
    fn from(err: E) -> Self {
        Self(err.into())
    }
}

impl axum::response::IntoResponse for ApiError {
    fn into_response(self) -> axum::response::Response {
        let body = Json(ApiResponse {
            ok: false,
            message: format!("{:#}", self.0),
        });
        (StatusCode::INTERNAL_SERVER_ERROR, body).into_response()
    }
}
