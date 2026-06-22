use anyhow::{anyhow, Result};
use axum::extract::{Path, State};
use axum::Json;
use chrono::{Duration, NaiveDate, Utc};
use jobs::api::ApiClient;
use jobs::patterns::detectors::{
    default_detectors, BottomTrendInflectionDetector, ImmortalGuidanceDetector,
    LimitUpPullbackDetector, LimitUpSidewaysDetector, MorningStarDetector,
    MultiGoldenCrossDetector, MultiPartyCannonDetector, PatternDetector,
    ResistanceBreakoutDetector, Strategy2560SelectionDetector, StrongWashWeakToStrongDetector,
    TrendAccelerationInflectionDetector, TrendResonanceReversalDetector, TrendStartDetector,
    WBottomDetector,
};
use jobs::patterns::{
    PatternDataSourceConfig, PatternScanFailure, PatternScanProgress, PatternScanReport,
    PatternScanRequest, PatternScanner, PatternSignal,
};
use jobs::utils::load_stock_codes_from_file;
use serde::{Deserialize, Serialize};
use tracing::{error, info, warn};

use super::app::{cleanup_market_scan_jobs, AppState, MarketScanJob, MarketScanJobStatus};
use super::errors::ApiError;

#[derive(Debug, Deserialize)]
pub struct PatternSingleRequest {
    pub symbol: String,
    pub trade_date: String,
    pub pattern_id: String,
    pub history_days: Option<i64>,
    pub refresh_remote: Option<bool>,
}

#[derive(Debug, Deserialize)]
pub struct PatternAllRequest {
    pub symbol: String,
    pub trade_date: String,
    pub history_days: Option<i64>,
    pub refresh_remote: Option<bool>,
}

#[derive(Debug, Deserialize)]
pub struct PatternMarketRequest {
    pub pattern_id: String,
    pub trade_date: String,
    pub history_days: Option<i64>,
    pub refresh_remote: Option<bool>,
    pub symbols: Option<Vec<String>>,
}

#[derive(Debug, Deserialize)]
pub struct PatternMarketAllRequest {
    pub trade_date: String,
    pub history_days: Option<i64>,
    pub refresh_remote: Option<bool>,
    pub symbols: Option<Vec<String>>,
}

#[derive(Debug, Serialize)]
pub struct PatternSingleResponse {
    pub symbol: String,
    pub trade_date: String,
    pub pattern_id: String,
    pub matched: bool,
    pub failed_symbols: Vec<PatternScanFailure>,
    pub signal: Option<PatternSignal>,
}

#[derive(Debug, Serialize)]
pub struct PatternAllResponse {
    pub symbol: String,
    pub trade_date: String,
    pub skipped_short_series: usize,
    pub signal_count: usize,
    pub failed_symbols: Vec<PatternScanFailure>,
    pub signals: Vec<PatternSignal>,
}

#[derive(Debug, Serialize)]
pub struct PatternMarketResponse {
    pub pattern_id: String,
    pub trade_date: String,
    pub requested_symbols: usize,
    pub series_count: usize,
    pub skipped_short_series: usize,
    pub signal_count: usize,
    pub fetched_symbols: usize,
    pub failed_symbols: Vec<PatternScanFailure>,
    pub signals: Vec<PatternSignal>,
}

#[derive(Debug, Serialize)]
pub struct PatternMarketJobAcceptedResponse {
    pub job_id: String,
    pub status: String,
}

#[derive(Debug, Serialize)]
pub struct PatternMarketJobResponse {
    pub job_id: String,
    pub pattern_id: String,
    pub trade_date: String,
    pub history_days: i64,
    pub refresh_remote: bool,
    pub status: String,
    pub requested_symbols: usize,
    pub resolved_symbols: usize,
    pub completed_symbols: usize,
    pub series_count: usize,
    pub skipped_short_series: usize,
    pub signal_count: usize,
    pub failed_symbols: Vec<PatternScanFailure>,
    pub error: Option<String>,
    pub created_at: String,
    pub started_at: Option<String>,
    pub finished_at: Option<String>,
    pub result: Option<PatternMarketResponse>,
}

#[derive(Debug, Serialize)]
pub struct PatternListResponse {
    pub pattern_ids: Vec<String>,
}

const FEISHU_SIGNAL_LIMIT: usize = 30;
const FEISHU_FAILURE_LIMIT: usize = 20;

pub async fn list_patterns() -> Json<PatternListResponse> {
    info!(target: "rstock::patterns", "list pattern ids");
    Json(PatternListResponse {
        pattern_ids: available_pattern_ids(),
    })
}

pub async fn check_single_pattern(
    State(state): State<AppState>,
    Json(req): Json<PatternSingleRequest>,
) -> Result<Json<PatternSingleResponse>, ApiError> {
    let trade_date = parse_request_date(&req.trade_date)?;
    info!(
        target: "rstock::patterns",
        symbol = %req.symbol,
        pattern_id = %req.pattern_id,
        trade_date = %trade_date,
        history_days = req.history_days.unwrap_or(365),
        refresh_remote = req.refresh_remote.unwrap_or(false),
        "check single pattern"
    );
    let scanner = build_scanner(&state, one_detector(&req.pattern_id)?)?;
    let report = run_scan(
        &scanner,
        vec![req.symbol.clone()],
        trade_date,
        req.history_days,
        req.refresh_remote,
    )
    .await?;
    let signal = report
        .signals
        .into_iter()
        .find(|signal| signal.symbol == req.symbol && signal.pattern_id == req.pattern_id);

    Ok(Json(PatternSingleResponse {
        symbol: req.symbol,
        trade_date: trade_date.format("%Y-%m-%d").to_string(),
        pattern_id: req.pattern_id,
        matched: signal.is_some(),
        failed_symbols: report.failed_symbols,
        signal,
    }))
}

pub async fn check_all_patterns(
    State(state): State<AppState>,
    Json(req): Json<PatternAllRequest>,
) -> Result<Json<PatternAllResponse>, ApiError> {
    let trade_date = parse_request_date(&req.trade_date)?;
    info!(
        target: "rstock::patterns",
        symbol = %req.symbol,
        trade_date = %trade_date,
        history_days = req.history_days.unwrap_or(365),
        refresh_remote = req.refresh_remote.unwrap_or(false),
        "check all patterns"
    );
    let scanner = build_scanner(&state, default_detectors())?;
    let report = run_scan(
        &scanner,
        vec![req.symbol.clone()],
        trade_date,
        req.history_days,
        req.refresh_remote,
    )
    .await?;

    dispatch_feishu_report(
        state.clone(),
        render_check_all_feishu_message(&req.symbol, trade_date, &report),
    );

    Ok(Json(PatternAllResponse {
        symbol: req.symbol,
        trade_date: trade_date.format("%Y-%m-%d").to_string(),
        skipped_short_series: report.skipped_short_series,
        signal_count: report.signal_count,
        failed_symbols: report.failed_symbols,
        signals: report.signals,
    }))
}

pub async fn scan_market_by_pattern(
    State(state): State<AppState>,
    Json(req): Json<PatternMarketRequest>,
) -> Result<Json<PatternMarketJobAcceptedResponse>, ApiError> {
    let trade_date = parse_request_date(&req.trade_date)?;
    let history_days = req.history_days.unwrap_or(365).max(30);
    let refresh_remote = req.refresh_remote.unwrap_or(false);
    let requested_symbols = req.symbols.as_ref().map(|items| items.len()).unwrap_or(0);
    let job_id = state.next_market_scan_job_id();
    info!(
        target: "rstock::patterns",
        job_id = %job_id,
        pattern_id = %req.pattern_id,
        trade_date = %trade_date,
        history_days,
        refresh_remote,
        requested_symbols,
        "scan market by pattern"
    );

    enqueue_market_scan_job(
        &state,
        job_id.clone(),
        req.pattern_id.clone(),
        trade_date,
        history_days,
        refresh_remote,
        requested_symbols,
    )
    .await;

    let task_state = state.clone();
    let pattern_id = req.pattern_id;
    let requested_symbols_input = req.symbols;
    let task_job_id = job_id.clone();
    tokio::spawn(async move {
        if let Err(err) = run_market_scan_job(
            task_state.clone(),
            task_job_id.clone(),
            pattern_id,
            trade_date,
            history_days,
            refresh_remote,
            requested_symbols_input,
        )
        .await
        {
            fail_market_scan_job(&task_state, &task_job_id, format!("{err:#}")).await;
        }
    });

    Ok(Json(PatternMarketJobAcceptedResponse {
        job_id,
        status: market_scan_job_status_label(MarketScanJobStatus::Queued).to_string(),
    }))
}

pub async fn scan_market_all_patterns(
    State(state): State<AppState>,
    Json(req): Json<PatternMarketAllRequest>,
) -> Result<Json<PatternMarketJobAcceptedResponse>, ApiError> {
    let trade_date = parse_request_date(&req.trade_date)?;
    let history_days = req.history_days.unwrap_or(365).max(30);
    let refresh_remote = req.refresh_remote.unwrap_or(false);
    let requested_symbols = req.symbols.as_ref().map(|items| items.len()).unwrap_or(0);
    let job_id = state.next_market_scan_job_id();
    let pattern_id = "all".to_string();
    info!(
        target: "rstock::patterns",
        job_id = %job_id,
        trade_date = %trade_date,
        history_days,
        refresh_remote,
        requested_symbols,
        "scan market all patterns"
    );

    enqueue_market_scan_job(
        &state,
        job_id.clone(),
        pattern_id.clone(),
        trade_date,
        history_days,
        refresh_remote,
        requested_symbols,
    )
    .await;

    let task_state = state.clone();
    let requested_symbols_input = req.symbols;
    let task_job_id = job_id.clone();
    tokio::spawn(async move {
        if let Err(err) = run_market_scan_all_patterns_job(
            task_state.clone(),
            task_job_id.clone(),
            pattern_id,
            trade_date,
            history_days,
            refresh_remote,
            requested_symbols_input,
        )
        .await
        {
            fail_market_scan_job(&task_state, &task_job_id, format!("{err:#}")).await;
        }
    });

    Ok(Json(PatternMarketJobAcceptedResponse {
        job_id,
        status: market_scan_job_status_label(MarketScanJobStatus::Queued).to_string(),
    }))
}

pub async fn get_market_scan_job(
    State(state): State<AppState>,
    Path(job_id): Path<String>,
) -> Result<Json<PatternMarketJobResponse>, ApiError> {
    let jobs = state.market_scan_jobs.lock().await;
    let job = jobs
        .get(&job_id)
        .cloned()
        .ok_or_else(|| ApiError::not_found(anyhow!("market scan job not found: {job_id}")))?;
    Ok(Json(pattern_market_job_response(job)))
}

fn build_scanner(
    state: &AppState,
    detectors: Vec<Box<dyn PatternDetector>>,
) -> Result<PatternScanner> {
    let mut data_source = PatternDataSourceConfig::new(state.args.base_url.clone());
    data_source.authorization = state.args.authorization.clone();
    data_source.timeout_secs = state.args.timeout;
    data_source.adjust_type = state.args.pattern_adjust_type.clone();
    data_source.tdx_fallback = state.args.pattern_tdx_fallback;
    data_source.batch_size = state.args.daily_chunk_size.max(1);
    data_source.fetch_concurrency = state.args.daily_fetch_concurrency.max(1);
    PatternScanner::new(data_source, detectors)
}

async fn run_scan(
    scanner: &PatternScanner,
    symbols: Vec<String>,
    trade_date: NaiveDate,
    history_days: Option<i64>,
    refresh_remote: Option<bool>,
) -> Result<PatternScanReport> {
    if symbols.is_empty() {
        return Err(anyhow!("symbols is empty"));
    }
    let days = history_days.unwrap_or(365).max(30);
    let start_date = trade_date - Duration::days(days);
    scanner
        .scan(PatternScanRequest {
            symbols,
            start_date,
            end_date: trade_date,
            refresh_remote: refresh_remote.unwrap_or(false),
        })
        .await
}

async fn run_scan_with_progress<F>(
    scanner: &PatternScanner,
    symbols: Vec<String>,
    trade_date: NaiveDate,
    history_days: i64,
    refresh_remote: bool,
    on_progress: F,
) -> Result<PatternScanReport>
where
    F: FnMut(PatternScanProgress) + Send,
{
    if symbols.is_empty() {
        return Err(anyhow!("symbols is empty"));
    }
    let start_date = trade_date - Duration::days(history_days);
    scanner
        .scan_with_progress(
            PatternScanRequest {
                symbols,
                start_date,
                end_date: trade_date,
                refresh_remote,
            },
            on_progress,
        )
        .await
}

async fn run_market_scan_job(
    state: AppState,
    job_id: String,
    pattern_id: String,
    trade_date: NaiveDate,
    history_days: i64,
    refresh_remote: bool,
    requested_symbols_input: Option<Vec<String>>,
) -> Result<()> {
    let symbols = match requested_symbols_input {
        Some(symbols) if !symbols.is_empty() => symbols,
        _ => discover_market_symbols(&state).await?,
    };
    info!(
        target: "rstock::patterns",
        job_id = %job_id,
        resolved_symbols = symbols.len(),
        "resolved market symbols"
    );
    mark_market_scan_job_running(&state, &job_id, symbols.len()).await;

    let scanner = build_scanner(&state, one_detector(&pattern_id)?)?;
    let progress_state = state.clone();
    let progress_job_id = job_id.clone();
    let notify_state = state.clone();
    let notify_job_id = job_id.clone();
    let report = run_scan_with_progress(
        &scanner,
        symbols,
        trade_date,
        history_days,
        refresh_remote,
        move |progress| {
            let progress_state = progress_state.clone();
            let progress_job_id = progress_job_id.clone();
            tokio::spawn(async move {
                update_market_scan_job_progress(&progress_state, &progress_job_id, progress).await;
            });
        },
    )
    .await?;
    dispatch_feishu_report(
        notify_state,
        render_market_scan_feishu_message(&notify_job_id, &pattern_id, trade_date, &report),
    );
    complete_market_scan_job(&state, &job_id, &pattern_id, trade_date, report).await;
    Ok(())
}

async fn run_market_scan_all_patterns_job(
    state: AppState,
    job_id: String,
    pattern_id: String,
    trade_date: NaiveDate,
    history_days: i64,
    refresh_remote: bool,
    requested_symbols_input: Option<Vec<String>>,
) -> Result<()> {
    let symbols = match requested_symbols_input {
        Some(symbols) if !symbols.is_empty() => symbols,
        _ => discover_market_symbols(&state).await?,
    };
    info!(
        target: "rstock::patterns",
        job_id = %job_id,
        resolved_symbols = symbols.len(),
        detectors = available_pattern_ids().len(),
        "resolved market symbols for all-pattern scan"
    );
    mark_market_scan_job_running(&state, &job_id, symbols.len()).await;

    let scanner = build_scanner(&state, default_detectors())?;
    let progress_state = state.clone();
    let progress_job_id = job_id.clone();
    let notify_state = state.clone();
    let notify_job_id = job_id.clone();
    let report = run_scan_with_progress(
        &scanner,
        symbols,
        trade_date,
        history_days,
        refresh_remote,
        move |progress| {
            let progress_state = progress_state.clone();
            let progress_job_id = progress_job_id.clone();
            tokio::spawn(async move {
                update_market_scan_job_progress(&progress_state, &progress_job_id, progress).await;
            });
        },
    )
    .await?;
    dispatch_feishu_report(
        notify_state,
        render_market_scan_feishu_message(&notify_job_id, &pattern_id, trade_date, &report),
    );
    complete_market_scan_job(&state, &job_id, &pattern_id, trade_date, report).await;
    Ok(())
}

async fn enqueue_market_scan_job(
    state: &AppState,
    job_id: String,
    pattern_id: String,
    trade_date: NaiveDate,
    history_days: i64,
    refresh_remote: bool,
    requested_symbols: usize,
) {
    let mut jobs = state.market_scan_jobs.lock().await;
    cleanup_market_scan_jobs(&mut jobs, Utc::now());
    jobs.insert(
        job_id.clone(),
        MarketScanJob {
            job_id,
            pattern_id,
            trade_date,
            history_days,
            refresh_remote,
            status: MarketScanJobStatus::Queued,
            requested_symbols,
            resolved_symbols: 0,
            completed_symbols: 0,
            series_count: 0,
            skipped_short_series: 0,
            signal_count: 0,
            failed_symbols: Vec::new(),
            result: None,
            error: None,
            created_at: Utc::now(),
            started_at: None,
            finished_at: None,
        },
    );
}

async fn mark_market_scan_job_running(state: &AppState, job_id: &str, resolved_symbols: usize) {
    let mut jobs = state.market_scan_jobs.lock().await;
    if let Some(job) = jobs.get_mut(job_id) {
        job.status = MarketScanJobStatus::Running;
        job.resolved_symbols = resolved_symbols;
        job.started_at = Some(Utc::now());
        job.failed_symbols.clear();
        job.error = None;
    }
}

async fn update_market_scan_job_progress(
    state: &AppState,
    job_id: &str,
    progress: PatternScanProgress,
) {
    let mut jobs = state.market_scan_jobs.lock().await;
    if let Some(job) = jobs.get_mut(job_id) {
        job.requested_symbols = progress.requested_symbols;
        job.resolved_symbols = progress.requested_symbols;
        job.completed_symbols = progress.completed_symbols;
        job.series_count = progress.series_count;
        job.skipped_short_series = progress.skipped_short_series;
        job.signal_count = progress.signal_count;
        if let Some(failure) = progress.latest_failure {
            job.failed_symbols.push(failure);
        }
    }
}

async fn complete_market_scan_job(
    state: &AppState,
    job_id: &str,
    pattern_id: &str,
    trade_date: NaiveDate,
    report: PatternScanReport,
) {
    let mut jobs = state.market_scan_jobs.lock().await;
    if let Some(job) = jobs.get_mut(job_id) {
        job.status = MarketScanJobStatus::Succeeded;
        job.requested_symbols = report.requested_symbols;
        job.resolved_symbols = report.requested_symbols;
        job.completed_symbols = report.requested_symbols;
        job.series_count = report.series_count;
        job.skipped_short_series = report.skipped_short_series;
        job.signal_count = report.signal_count;
        job.failed_symbols = report.failed_symbols.clone();
        job.result = Some(report.clone());
        job.error = None;
        job.finished_at = Some(Utc::now());
        info!(
            target: "rstock::patterns",
            job_id = %job_id,
            pattern_id,
            trade_date = %trade_date,
            requested_symbols = report.requested_symbols,
            signal_count = report.signal_count,
            "market scan job completed"
        );
    }
    cleanup_market_scan_jobs(&mut jobs, Utc::now());
}

async fn fail_market_scan_job(state: &AppState, job_id: &str, error: String) {
    let mut notify_payload = None;
    let mut jobs = state.market_scan_jobs.lock().await;
    if let Some(job) = jobs.get_mut(job_id) {
        error!(
            target: "rstock::patterns",
            job_id = %job_id,
            pattern_id = %job.pattern_id,
            trade_date = %job.trade_date,
            error = %error,
            "market scan job failed"
        );
        job.status = MarketScanJobStatus::Failed;
        job.error = Some(error);
        job.finished_at = Some(Utc::now());
        notify_payload = Some(render_market_scan_failed_feishu_message(job));
    }
    cleanup_market_scan_jobs(&mut jobs, Utc::now());
    drop(jobs);

    if let Some(message) = notify_payload {
        dispatch_feishu_report(state.clone(), message);
    }
}

fn pattern_market_job_response(job: MarketScanJob) -> PatternMarketJobResponse {
    let MarketScanJob {
        job_id,
        pattern_id,
        trade_date,
        history_days,
        refresh_remote,
        status,
        requested_symbols,
        resolved_symbols,
        completed_symbols,
        series_count,
        skipped_short_series,
        signal_count,
        failed_symbols,
        result,
        error,
        created_at,
        started_at,
        finished_at,
    } = job;
    PatternMarketJobResponse {
        job_id,
        pattern_id: pattern_id.clone(),
        trade_date: trade_date.format("%Y-%m-%d").to_string(),
        history_days,
        refresh_remote,
        status: market_scan_job_status_label(status).to_string(),
        requested_symbols,
        resolved_symbols,
        completed_symbols,
        series_count,
        skipped_short_series,
        signal_count,
        failed_symbols,
        error,
        created_at: created_at.to_rfc3339(),
        started_at: started_at.map(|value| value.to_rfc3339()),
        finished_at: finished_at.map(|value| value.to_rfc3339()),
        result: result.map(|report| pattern_market_response(pattern_id, trade_date, report)),
    }
}

fn pattern_market_response(
    pattern_id: String,
    trade_date: NaiveDate,
    report: PatternScanReport,
) -> PatternMarketResponse {
    PatternMarketResponse {
        pattern_id,
        trade_date: trade_date.format("%Y-%m-%d").to_string(),
        requested_symbols: report.requested_symbols,
        series_count: report.series_count,
        skipped_short_series: report.skipped_short_series,
        signal_count: report.signal_count,
        fetched_symbols: report.fetched_symbols.len(),
        failed_symbols: report.failed_symbols,
        signals: report.signals,
    }
}

fn market_scan_job_status_label(status: MarketScanJobStatus) -> &'static str {
    match status {
        MarketScanJobStatus::Queued => "queued",
        MarketScanJobStatus::Running => "running",
        MarketScanJobStatus::Succeeded => "succeeded",
        MarketScanJobStatus::Failed => "failed",
    }
}

async fn discover_market_symbols(state: &AppState) -> Result<Vec<String>> {
    if let Some(path) = state.args.daily_stock_codes_file.as_ref() {
        return load_stock_codes_from_file(path);
    }

    let started_at = std::time::Instant::now();
    info!(
        target: "rstock::patterns",
        timeout_secs = state.args.timeout,
        "discover market symbols from qmt"
    );
    let api = ApiClient::new(
        state.args.base_url.clone(),
        state.args.authorization.clone(),
        std::time::Duration::from_secs(state.args.timeout),
    )?;
    let symbols = api.discover_all_stock_codes().await?;
    info!(
        target: "rstock::patterns",
        resolved_symbols = symbols.len(),
        discover_market_symbols_ms = started_at.elapsed().as_millis(),
        "discovered market symbols from qmt"
    );
    Ok(symbols)
}

fn parse_request_date(input: &str) -> Result<NaiveDate> {
    let trimmed = input.trim();
    if trimmed.is_empty() {
        return Err(anyhow!("trade_date is empty"));
    }
    NaiveDate::parse_from_str(trimmed, "%Y-%m-%d")
        .or_else(|_| NaiveDate::parse_from_str(trimmed, "%Y%m%d"))
        .map_err(|err| anyhow!("invalid trade_date {trimmed}: {err}"))
}

fn one_detector(pattern_id: &str) -> Result<Vec<Box<dyn PatternDetector>>> {
    let detector: Box<dyn PatternDetector> = match pattern_id.trim() {
        "bottom_trend_inflection" => Box::new(BottomTrendInflectionDetector::default()),
        "immortal_guidance" => Box::new(ImmortalGuidanceDetector::default()),
        "limit_up_pullback" => Box::new(LimitUpPullbackDetector::default()),
        "limit_up_sideways" => Box::new(LimitUpSidewaysDetector::default()),
        "morning_star" => Box::new(MorningStarDetector::default()),
        "multi_golden_cross" => Box::new(MultiGoldenCrossDetector::default()),
        "multi_party_cannon" => Box::new(MultiPartyCannonDetector::default()),
        "resistance_breakout" => Box::new(ResistanceBreakoutDetector::default()),
        "strategy_2560_selection" => Box::new(Strategy2560SelectionDetector::default()),
        "strong_wash_weak_to_strong" => Box::new(StrongWashWeakToStrongDetector::default()),
        "trend_acceleration_inflection" => Box::new(TrendAccelerationInflectionDetector::default()),
        "trend_resonance_reversal" => Box::new(TrendResonanceReversalDetector::default()),
        "trend_start" => Box::new(TrendStartDetector::default()),
        "w_bottom" => Box::new(WBottomDetector::default()),
        other => {
            let available = available_pattern_ids().join(", ");
            return Err(anyhow!(
                "unknown pattern_id {other}, available pattern ids: {available}"
            ));
        }
    };
    Ok(vec![detector])
}

fn available_pattern_ids() -> Vec<String> {
    let mut ids = default_detectors()
        .into_iter()
        .map(|detector| detector.id().to_string())
        .collect::<Vec<_>>();
    ids.sort();
    ids
}

fn dispatch_feishu_report(state: AppState, message: String) {
    if !state.feishu.is_enabled() {
        return;
    }

    tokio::spawn(async move {
        if let Err(err) = state.feishu.send_text(message).await {
            warn!(target: "rstock::patterns", error = %err, "send feishu report failed");
        }
    });
}

fn render_check_all_feishu_message(
    symbol: &str,
    trade_date: NaiveDate,
    report: &PatternScanReport,
) -> String {
    let mut lines = vec![
        "Pattern Check-All Finished".to_string(),
        format!("Trade Date: {}", trade_date.format("%Y-%m-%d")),
        format!("Symbol: {symbol}"),
        format!(
            "Summary: matched {} patterns, skipped {} short series, failed {} symbols",
            report.signal_count,
            report.skipped_short_series,
            report.failed_symbols.len()
        ),
        String::new(),
    ];
    append_signal_section(
        &mut lines,
        "Matched Patterns",
        &report.signals,
        FEISHU_SIGNAL_LIMIT,
    );
    append_failure_section(&mut lines, &report.failed_symbols, FEISHU_FAILURE_LIMIT);
    lines.join("\n")
}

fn render_market_scan_feishu_message(
    job_id: &str,
    pattern_id: &str,
    trade_date: NaiveDate,
    report: &PatternScanReport,
) -> String {
    let mut lines = vec![
        "Pattern Market Scan Finished".to_string(),
        format!("Job ID: {job_id}"),
        format!("Trade Date: {}", trade_date.format("%Y-%m-%d")),
        format!("Pattern: {pattern_id}"),
        format!(
            "Summary: requested {}, completed {}, matched {}, fetched {}, failed {}",
            report.requested_symbols,
            report.requested_symbols,
            report.signal_count,
            report.fetched_symbols.len(),
            report.failed_symbols.len()
        ),
        format!("Skipped Short Series: {}", report.skipped_short_series),
        String::new(),
    ];
    append_signal_section(
        &mut lines,
        "Matched Signals",
        &report.signals,
        FEISHU_SIGNAL_LIMIT,
    );
    append_failure_section(&mut lines, &report.failed_symbols, FEISHU_FAILURE_LIMIT);
    lines.join("\n")
}

fn render_market_scan_failed_feishu_message(job: &MarketScanJob) -> String {
    let mut lines = vec![
        "Pattern Market Scan Failed".to_string(),
        format!("Job ID: {}", job.job_id),
        format!("Trade Date: {}", job.trade_date.format("%Y-%m-%d")),
        format!("Pattern: {}", job.pattern_id),
        format!("Requested Symbols: {}", job.requested_symbols),
        format!("Completed Symbols: {}", job.completed_symbols),
    ];

    if let Some(error) = job.error.as_ref() {
        lines.push(format!("Error: {}", flatten_text(error)));
    }

    if !job.failed_symbols.is_empty() {
        lines.push(String::new());
        append_failure_section(&mut lines, &job.failed_symbols, FEISHU_FAILURE_LIMIT);
    }

    lines.join("\n")
}

fn append_signal_section(
    lines: &mut Vec<String>,
    title: &str,
    signals: &[PatternSignal],
    limit: usize,
) {
    lines.push(format!("{title}:"));
    if signals.is_empty() {
        lines.push("- None".to_string());
        lines.push(String::new());
        return;
    }

    for signal in signals.iter().take(limit) {
        lines.push(format_signal_line(signal));
    }
    append_omitted_count(lines, signals.len(), limit);
    lines.push(String::new());
}

fn append_failure_section(lines: &mut Vec<String>, failures: &[PatternScanFailure], limit: usize) {
    lines.push("Failed Symbols:".to_string());
    if failures.is_empty() {
        lines.push("- None".to_string());
        return;
    }

    for failure in failures.iter().take(limit) {
        lines.push(format!(
            "- {} | {}",
            failure.symbol,
            flatten_text(&failure.error)
        ));
    }
    append_omitted_count(lines, failures.len(), limit);
}

fn append_omitted_count(lines: &mut Vec<String>, actual: usize, limit: usize) {
    if actual > limit {
        lines.push(format!("- ... and {} more", actual - limit));
    }
}

fn format_signal_line(signal: &PatternSignal) -> String {
    let explanation = flatten_text(&signal.explanation);
    format!(
        "- {} | {} | {} | score {:.2} | {}",
        signal.symbol,
        signal.pattern_id,
        signal.signal_date.format("%Y-%m-%d"),
        signal.score,
        explanation
    )
}

fn flatten_text(input: &str) -> String {
    input.split_whitespace().collect::<Vec<_>>().join(" ")
}
