use anyhow::{anyhow, Result};
use axum::extract::State;
use axum::Json;
use chrono::{Duration, NaiveDate};
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
    PatternDataSourceConfig, PatternScanReport, PatternScanRequest, PatternScanner, PatternSignal,
};
use jobs::utils::load_stock_codes_from_file;
use serde::{Deserialize, Serialize};
use tracing::info;

use super::app::AppState;
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

#[derive(Debug, Serialize)]
pub struct PatternSingleResponse {
    pub symbol: String,
    pub trade_date: String,
    pub pattern_id: String,
    pub matched: bool,
    pub signal: Option<PatternSignal>,
}

#[derive(Debug, Serialize)]
pub struct PatternAllResponse {
    pub symbol: String,
    pub trade_date: String,
    pub skipped_short_series: usize,
    pub signal_count: usize,
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
    pub signals: Vec<PatternSignal>,
}

#[derive(Debug, Serialize)]
pub struct PatternListResponse {
    pub pattern_ids: Vec<String>,
}

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

    Ok(Json(PatternAllResponse {
        symbol: req.symbol,
        trade_date: trade_date.format("%Y-%m-%d").to_string(),
        skipped_short_series: report.skipped_short_series,
        signal_count: report.signal_count,
        signals: report.signals,
    }))
}

pub async fn scan_market_by_pattern(
    State(state): State<AppState>,
    Json(req): Json<PatternMarketRequest>,
) -> Result<Json<PatternMarketResponse>, ApiError> {
    let trade_date = parse_request_date(&req.trade_date)?;
    let requested_symbols = req.symbols.as_ref().map(|items| items.len()).unwrap_or(0);
    info!(
        target: "rstock::patterns",
        pattern_id = %req.pattern_id,
        trade_date = %trade_date,
        history_days = req.history_days.unwrap_or(365),
        refresh_remote = req.refresh_remote.unwrap_or(false),
        requested_symbols,
        "scan market by pattern"
    );
    let symbols = match req.symbols {
        Some(symbols) if !symbols.is_empty() => symbols,
        _ => discover_market_symbols(&state).await?,
    };
    info!(
        target: "rstock::patterns",
        resolved_symbols = symbols.len(),
        "resolved market symbols"
    );
    let scanner = build_scanner(&state, one_detector(&req.pattern_id)?)?;
    let report = run_scan(
        &scanner,
        symbols,
        trade_date,
        req.history_days,
        req.refresh_remote,
    )
    .await?;

    Ok(Json(PatternMarketResponse {
        pattern_id: req.pattern_id,
        trade_date: trade_date.format("%Y-%m-%d").to_string(),
        requested_symbols: report.requested_symbols,
        series_count: report.series_count,
        skipped_short_series: report.skipped_short_series,
        signal_count: report.signal_count,
        fetched_symbols: report.fetched_symbols.len(),
        signals: report.signals,
    }))
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

async fn discover_market_symbols(state: &AppState) -> Result<Vec<String>> {
    if let Some(path) = state.args.daily_stock_codes_file.as_ref() {
        return load_stock_codes_from_file(path);
    }

    let api = ApiClient::new(
        state.args.base_url.clone(),
        state.args.authorization.clone(),
        std::time::Duration::from_secs(state.args.timeout),
    )?;
    api.discover_all_stock_codes().await
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
