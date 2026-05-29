mod app;
mod config;
mod errors;

use std::path::PathBuf;
use std::str::FromStr;

use anyhow::{Context, Result};
use apalis::prelude::*;
use apalis_cron::{CronContext, CronStream, Schedule};
use axum::extract::State;
use axum::routing::{get, post};
use axum::{Json, Router};
use chrono::{Local, Utc};
use serde::Deserialize;
use tokio::net::TcpListener;

use self::app::AppState;
pub use self::config::ServerConfig;
use self::errors::{ok, ApiError, ApiResponse};
use jobs::sync_daily::{run_sync_daily, SyncDailyArgs};
use jobs::sync_minute::{run_sync_minute, SyncMinuteArgs};

#[derive(Debug, Deserialize)]
struct SyncRequest {
    date: Option<String>,
    start_date: Option<String>,
    end_date: Option<String>,
}

#[derive(Default, Debug, Clone)]
struct DailyCron;

#[derive(Default, Debug, Clone)]
struct MinuteCron;

pub async fn run_server(args: ServerConfig) -> Result<()> {
    let state = AppState::new(args);
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
        stock_codes_file: args.daily_stock_codes_file.clone(),
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
        stock_codes_file: args.minute_stock_codes_file.clone(),
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
