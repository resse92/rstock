use std::panic::AssertUnwindSafe;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, Context, Result};
use chrono::NaiveDate;
use futures_util::FutureExt;
use polars::prelude::DataFrame;
use tokio::task::JoinSet;

use crate::api::ApiClient;
use crate::kline_frame::{concat_frames, frame_symbols};
use crate::models::MarketRequest;
use crate::patterns::detectors::PatternDetector;
use crate::patterns::indicators::SeriesIndicators;
use crate::patterns::model::{
    BarSeries, PatternScanFailure, PatternScanReport, PatternScanRequest, PatternSignal,
};
use crate::tdx_source;
use tracing::{info, warn};

#[derive(Debug, Clone)]
pub struct PatternDataSourceConfig {
    pub base_url: String,
    pub authorization: Option<String>,
    pub timeout_secs: u64,
    pub adjust_type: String,
    pub tdx_fallback: bool,
    pub batch_size: usize,
    pub fetch_concurrency: usize,
}

impl PatternDataSourceConfig {
    pub fn new(base_url: impl Into<String>) -> Self {
        Self {
            base_url: base_url.into(),
            authorization: None,
            timeout_secs: 30,
            adjust_type: "qfq".to_string(),
            tdx_fallback: true,
            batch_size: 200,
            fetch_concurrency: 4,
        }
    }
}

pub struct PatternScanner {
    detectors: Arc<Vec<Box<dyn PatternDetector>>>,
    data_source: PatternDataSourceConfig,
}

#[derive(Debug, Clone)]
pub struct PatternScanProgress {
    pub requested_symbols: usize,
    pub completed_symbols: usize,
    pub series_count: usize,
    pub skipped_short_series: usize,
    pub signal_count: usize,
    pub failed_symbols: usize,
    pub latest_failure: Option<PatternScanFailure>,
}

impl PatternScanner {
    pub fn new(
        data_source: PatternDataSourceConfig,
        detectors: Vec<Box<dyn PatternDetector>>,
    ) -> Result<Self> {
        Ok(Self {
            detectors: Arc::new(detectors),
            data_source,
        })
    }

    pub async fn scan(&self, request: PatternScanRequest) -> Result<PatternScanReport> {
        self.scan_with_progress(request, |_| {}).await
    }

    pub async fn scan_with_progress<F>(
        &self,
        request: PatternScanRequest,
        mut on_progress: F,
    ) -> Result<PatternScanReport>
    where
        F: FnMut(PatternScanProgress) + Send,
    {
        if request.symbols.is_empty() {
            return Err(anyhow!("pattern scan request symbols is empty"));
        }
        if request.start_date > request.end_date {
            return Err(anyhow!("start_date cannot be later than end_date"));
        }
        info!(
            target: "rstock_jobs::patterns",
            symbols = request.symbols.len(),
            detectors = self.detectors.len(),
            start_date = %request.start_date,
            end_date = %request.end_date,
            refresh_remote = request.refresh_remote,
            "pattern scan start"
        );
        let fetch_concurrency = self.data_source.fetch_concurrency.max(1);
        let mut next_symbol_idx = 0usize;
        let mut join_set = JoinSet::new();
        let mut series_count = 0usize;
        let mut skipped_short_series = 0usize;
        let mut fetched_symbols = Vec::<String>::new();
        let mut failed_symbols = Vec::<PatternScanFailure>::new();
        let mut signals = Vec::<PatternSignal>::new();
        let requested_symbols = request.symbols.len();
        let mut completed_symbols = 0usize;

        while next_symbol_idx < request.symbols.len() || !join_set.is_empty() {
            while next_symbol_idx < request.symbols.len() && join_set.len() < fetch_concurrency {
                let symbol = request.symbols[next_symbol_idx].clone();
                let start_date = request.start_date;
                let end_date = request.end_date;
                let data_source = self.data_source.clone();
                let detectors = Arc::clone(&self.detectors);
                join_set.spawn(async move {
                    let worker_symbol = symbol.clone();
                    match AssertUnwindSafe(async move {
                        let bars = fetch_remote_bars_with_config(
                            data_source,
                            std::slice::from_ref(&symbol),
                            start_date,
                            end_date,
                        )
                        .await?;
                        Ok::<_, anyhow::Error>(scan_symbol(symbol, bars, detectors.as_ref()))
                    })
                    .catch_unwind()
                    .await
                    {
                        Ok(Ok(result)) => result,
                        Ok(Err(err)) => symbol_scan_failure(worker_symbol, err),
                        Err(panic) => symbol_scan_failure(
                            worker_symbol,
                            anyhow!("pattern symbol worker panicked: {}", panic_message(&panic)),
                        ),
                    }
                });
                next_symbol_idx += 1;
            }

            if let Some(result) = join_set.join_next().await {
                let result = match result {
                    Ok(result) => result,
                    Err(err) => {
                        let failure = PatternScanFailure {
                            symbol: "<unknown>".to_string(),
                            error: format!("pattern symbol worker join failed: {err}"),
                        };
                        warn!(
                            target: "rstock_jobs::patterns",
                            error = %failure.error,
                            "pattern symbol worker join failed"
                        );
                        failed_symbols.push(failure.clone());
                        completed_symbols += 1;
                        on_progress(PatternScanProgress {
                            requested_symbols,
                            completed_symbols,
                            series_count,
                            skipped_short_series,
                            signal_count: signals.len(),
                            failed_symbols: failed_symbols.len(),
                            latest_failure: Some(failure),
                        });
                        continue;
                    }
                };
                completed_symbols += 1;
                if result.had_series {
                    series_count += 1;
                    fetched_symbols.push(result.symbol.clone());
                }
                if result.skipped_short {
                    skipped_short_series += 1;
                }
                if let Some(failure) = result.failure.clone() {
                    failed_symbols.push(failure.clone());
                }
                signals.extend(result.signals);
                on_progress(PatternScanProgress {
                    requested_symbols,
                    completed_symbols,
                    series_count,
                    skipped_short_series,
                    signal_count: signals.len(),
                    failed_symbols: failed_symbols.len(),
                    latest_failure: result.failure,
                });
            }
        }

        info!(
            target: "rstock_jobs::patterns",
            requested_symbols = request.symbols.len(),
            series_count,
            skipped_short_series,
            signal_count = signals.len(),
            failed_symbols = failed_symbols.len(),
            fetched_symbols = fetched_symbols.len(),
            "pattern scan done"
        );
        Ok(PatternScanReport {
            requested_symbols,
            skipped_short_series,
            series_count,
            signal_count: signals.len(),
            fetched_symbols,
            failed_symbols,
            signals,
        })
    }
}

async fn fetch_remote_bars_with_config(
    data_source: PatternDataSourceConfig,
    symbols: &[String],
    start_date: NaiveDate,
    end_date: NaiveDate,
) -> Result<DataFrame> {
    if symbols.is_empty() {
        return Ok(DataFrame::default());
    }

    let api = ApiClient::new(
        data_source.base_url.clone(),
        data_source.authorization.clone(),
        Duration::from_secs(data_source.timeout_secs),
    )?;
    let batch_size = data_source.batch_size.max(1);
    let fetch_concurrency = data_source.fetch_concurrency.max(1);
    let symbol_batches = symbols
        .chunks(batch_size)
        .map(|chunk| chunk.to_vec())
        .collect::<Vec<_>>();
    let start = compact_date(start_date);
    let end = compact_date(end_date);
    let mut frames = Vec::new();
    let mut next_batch_idx = 0usize;
    let mut join_set = JoinSet::new();

    while next_batch_idx < symbol_batches.len() || !join_set.is_empty() {
        while next_batch_idx < symbol_batches.len() && join_set.len() < fetch_concurrency {
            let batch_idx = next_batch_idx;
            let batch = symbol_batches[next_batch_idx].clone();
            let api_clone = api.clone();
            let start_clone = start.clone();
            let end_clone = end.clone();
            let adjust_type = data_source.adjust_type.clone();
            let tdx_fallback = data_source.tdx_fallback;
            info!(
                target: "rstock_jobs::patterns",
                batch = batch_idx + 1,
                size = batch.len(),
                start_date = %start_clone,
                end_date = %end_clone,
                "fetch remote bars batch"
            );
            join_set.spawn(async move {
                let frame = fetch_batch_with_split(
                    &api_clone,
                    batch_idx + 1,
                    batch.clone(),
                    &start_clone,
                    &end_clone,
                    &adjust_type,
                )
                .await?;
                info!(
                    target: "rstock_jobs::patterns",
                    batch = batch_idx + 1,
                    qmt_daily_bars = frame.height(),
                    "qmt batch fetched"
                );
                if tdx_fallback && frame.height() == 0 {
                    info!(
                        target: "rstock_jobs::patterns",
                        batch = batch_idx + 1,
                        qmt_missing_symbols = batch.len(),
                        "qmt returned no bars, fallback to tdx"
                    );
                    let fallback =
                        tdx_source::fetch_daily_bars_frame(&batch, &start_clone, &end_clone)?;
                    info!(
                        target: "rstock_jobs::patterns",
                        batch = batch_idx + 1,
                        tdx_daily_bars = fallback.height(),
                        "tdx fallback fetched"
                    );
                    return Ok::<DataFrame, anyhow::Error>(fallback);
                }

                Ok::<DataFrame, anyhow::Error>(frame)
            });
            next_batch_idx += 1;
        }

        if let Some(result) = join_set.join_next().await {
            frames.push(result.context("pattern fetch worker join failed")??);
        }
    }

    let frame = concat_frames(frames)?;
    info!(
        target: "rstock_jobs::patterns",
        total_normalized_bars = frame.height(),
        "remote bars normalized"
    );
    Ok(frame)
}

fn compact_date(date: NaiveDate) -> String {
    date.format("%Y%m%d").to_string()
}

struct SymbolScanResult {
    symbol: String,
    had_series: bool,
    skipped_short: bool,
    failure: Option<PatternScanFailure>,
    signals: Vec<PatternSignal>,
}

fn scan_symbol(
    symbol: String,
    frame: DataFrame,
    detectors: &[Box<dyn PatternDetector>],
) -> SymbolScanResult {
    let exchange = frame
        .column("exchange")
        .ok()
        .and_then(|col| col.str().ok())
        .and_then(|col| col.get(0))
        .unwrap_or_default()
        .to_string();
    let Ok(series) = BarSeries::from_frame(symbol.clone(), exchange, frame) else {
        return SymbolScanResult {
            symbol,
            had_series: false,
            skipped_short: false,
            failure: None,
            signals: Vec::new(),
        };
    };
    if series.is_empty() {
        info!(
            target: "rstock_jobs::patterns",
            symbol = %symbol,
            "no bars returned for symbol"
        );
        return SymbolScanResult {
            symbol,
            had_series: false,
            skipped_short: false,
            failure: None,
            signals: Vec::new(),
        };
    }
    if series.len() < 20 {
        info!(
            target: "rstock_jobs::patterns",
            symbol = %symbol,
            bars = series.len(),
            "skipped insufficient bars"
        );
        return SymbolScanResult {
            symbol,
            had_series: true,
            skipped_short: true,
            failure: None,
            signals: Vec::new(),
        };
    }

    let indicators = SeriesIndicators::calculate(&series);
    let signals = detectors
        .iter()
        .filter_map(|detector| detector.detect(&series, &indicators))
        .collect::<Vec<_>>();
    info!(
        target: "rstock_jobs::patterns",
        symbol = %symbol,
        bars = series.len(),
        matched = signals.len(),
        "pattern scan symbol complete"
    );
    SymbolScanResult {
        symbol,
        had_series: true,
        skipped_short: false,
        failure: None,
        signals,
    }
}

fn symbol_scan_failure(symbol: String, err: anyhow::Error) -> SymbolScanResult {
    let error = format!("{err:#}");
    warn!(
        target: "rstock_jobs::patterns",
        symbol = %symbol,
        error = %error,
        "pattern scan symbol failed"
    );
    SymbolScanResult {
        symbol: symbol.clone(),
        had_series: false,
        skipped_short: false,
        failure: Some(PatternScanFailure { symbol, error }),
        signals: Vec::new(),
    }
}

fn panic_message(payload: &(dyn std::any::Any + Send)) -> String {
    if let Some(message) = payload.downcast_ref::<&str>() {
        return (*message).to_string();
    }
    if let Some(message) = payload.downcast_ref::<String>() {
        return message.clone();
    }
    "unknown panic payload".to_string()
}

async fn fetch_batch_with_split(
    api: &ApiClient,
    batch_no: usize,
    symbols: Vec<String>,
    start_date: &str,
    end_date: &str,
    adjust_type: &str,
) -> Result<DataFrame> {
    let request = MarketRequest::new(
        symbols.clone(),
        "1d",
        start_date.to_string(),
        end_date.to_string(),
        adjust_type.to_string(),
    );

    match api.fetch_kline_frame(&request).await {
        Ok(frame) => {
            let found = frame_symbols(&frame)?;
            let missing = symbols
                .iter()
                .filter(|code| !found.contains(*code))
                .cloned()
                .collect::<Vec<_>>();
            if missing.is_empty() {
                Ok(frame)
            } else {
                let fallback = tdx_source::fetch_daily_bars_frame(&missing, start_date, end_date)?;
                concat_frames(vec![frame, fallback])
            }
        }
        Err(err) if symbols.len() > 1 => {
            let mid = symbols.len() / 2;
            let left = symbols[..mid].to_vec();
            let right = symbols[mid..].to_vec();
            info!(
                target: "rstock_jobs::patterns",
                batch = batch_no,
                size = symbols.len(),
                left_size = left.len(),
                right_size = right.len(),
                error = %err,
                "qmt batch failed, splitting batch"
            );
            let left_frame = Box::pin(fetch_batch_with_split(
                api,
                batch_no,
                left,
                start_date,
                end_date,
                adjust_type,
            ))
            .await?;
            let right_frame = Box::pin(fetch_batch_with_split(
                api,
                batch_no,
                right,
                start_date,
                end_date,
                adjust_type,
            ))
            .await?;
            concat_frames(vec![left_frame, right_frame])
        }
        Err(err) => Err(err).with_context(|| {
            format!(
                "failed to fetch daily bars from qmt for batch size {}",
                symbols.len()
            )
        }),
    }
}
