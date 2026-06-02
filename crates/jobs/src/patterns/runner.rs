use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, Context, Result};
use chrono::NaiveDate;
use tokio::task::JoinSet;

use crate::api::ApiClient;
use crate::models::{DailyBar, MarketRequest};
use crate::normalize::normalize_full_kline_response;
use crate::patterns::detectors::PatternDetector;
use crate::patterns::indicators::SeriesIndicators;
use crate::patterns::model::{
    Bar, BarSeries, PatternScanReport, PatternScanRequest, PatternSignal,
};
use crate::tdx_source;
use tracing::info;

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
        let mut signals = Vec::<PatternSignal>::new();

        while next_symbol_idx < request.symbols.len() || !join_set.is_empty() {
            while next_symbol_idx < request.symbols.len() && join_set.len() < fetch_concurrency {
                let symbol = request.symbols[next_symbol_idx].clone();
                let start_date = request.start_date;
                let end_date = request.end_date;
                let data_source = self.data_source.clone();
                let detectors = Arc::clone(&self.detectors);
                join_set.spawn(async move {
                    let bars = fetch_remote_bars_with_config(
                        data_source,
                        std::slice::from_ref(&symbol),
                        start_date,
                        end_date,
                    )
                    .await?;
                    Ok::<_, anyhow::Error>(scan_symbol(symbol, bars, detectors.as_ref()))
                });
                next_symbol_idx += 1;
            }

            if let Some(result) = join_set.join_next().await {
                let result = result.context("pattern symbol worker join failed")??;
                if result.had_series {
                    series_count += 1;
                    fetched_symbols.push(result.symbol.clone());
                }
                if result.skipped_short {
                    skipped_short_series += 1;
                }
                signals.extend(result.signals);
            }
        }

        info!(
            target: "rstock_jobs::patterns",
            requested_symbols = request.symbols.len(),
            series_count,
            skipped_short_series,
            signal_count = signals.len(),
            fetched_symbols = fetched_symbols.len(),
            "pattern scan done"
        );
        Ok(PatternScanReport {
            requested_symbols: request.symbols.len(),
            skipped_short_series,
            series_count,
            signal_count: signals.len(),
            fetched_symbols,
            signals,
        })
    }
}

async fn fetch_remote_bars_with_config(
    data_source: PatternDataSourceConfig,
    symbols: &[String],
    start_date: NaiveDate,
    end_date: NaiveDate,
) -> Result<Vec<Bar>> {
    if symbols.is_empty() {
        return Ok(Vec::new());
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
    let mut all_daily_bars = Vec::new();
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
                let mut daily_bars = fetch_batch_with_split(
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
                    qmt_daily_bars = daily_bars.len(),
                    "qmt batch fetched"
                );
                if tdx_fallback && daily_bars.is_empty() {
                    info!(
                        target: "rstock_jobs::patterns",
                        batch = batch_idx + 1,
                        qmt_missing_symbols = batch.len(),
                        "qmt returned no bars, fallback to tdx"
                    );
                    let fallback = tdx_source::fetch_daily_bars(&batch, &start_clone, &end_clone)?;
                    info!(
                        target: "rstock_jobs::patterns",
                        batch = batch_idx + 1,
                        tdx_daily_bars = fallback.len(),
                        "tdx fallback fetched"
                    );
                    daily_bars.extend(fallback);
                }

                Ok::<Vec<DailyBar>, anyhow::Error>(daily_bars)
            });
            next_batch_idx += 1;
        }

        if let Some(result) = join_set.join_next().await {
            let batch_bars = result.context("pattern fetch worker join failed")??;
            all_daily_bars.extend(batch_bars);
        }
    }

    let mut bars = Vec::new();
    for daily_bar in all_daily_bars {
        if let Some(bar) = Bar::from_daily_bar(daily_bar)? {
            bars.push(bar);
        }
    }
    info!(
        target: "rstock_jobs::patterns",
        total_normalized_bars = bars.len(),
        "remote bars normalized"
    );
    Ok(bars)
}

fn compact_date(date: NaiveDate) -> String {
    date.format("%Y%m%d").to_string()
}

struct SymbolScanResult {
    symbol: String,
    had_series: bool,
    skipped_short: bool,
    signals: Vec<PatternSignal>,
}

fn scan_symbol(
    symbol: String,
    bars: Vec<Bar>,
    detectors: &[Box<dyn PatternDetector>],
) -> SymbolScanResult {
    if bars.is_empty() {
        info!(
            target: "rstock_jobs::patterns",
            symbol = %symbol,
            "no bars returned for symbol"
        );
        return SymbolScanResult {
            symbol,
            had_series: false,
            skipped_short: false,
            signals: Vec::new(),
        };
    }

    let exchange = bars
        .first()
        .map(|bar| bar.exchange.clone())
        .unwrap_or_default();
    let series = BarSeries::new(symbol.clone(), exchange, bars);
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
        signals,
    }
}

async fn fetch_batch_with_split(
    api: &ApiClient,
    batch_no: usize,
    symbols: Vec<String>,
    start_date: &str,
    end_date: &str,
    adjust_type: &str,
) -> Result<Vec<DailyBar>> {
    let request = MarketRequest::new(
        symbols.clone(),
        "1d",
        start_date.to_string(),
        end_date.to_string(),
        adjust_type.to_string(),
    );

    match api.fetch_market_batch(&request).await {
        Ok(payload) => {
            let mut daily_bars = normalize_full_kline_response(&payload, "1d")
                .into_iter()
                .filter_map(|row| DailyBar::from_normalized(&row))
                .collect::<Vec<_>>();
            for bar in &mut daily_bars {
                bar.source = Some("qmt".to_string());
            }
            Ok(daily_bars)
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
            let mut left_bars = Box::pin(fetch_batch_with_split(
                api,
                batch_no,
                left,
                start_date,
                end_date,
                adjust_type,
            ))
            .await?;
            let right_bars = Box::pin(fetch_batch_with_split(
                api,
                batch_no,
                right,
                start_date,
                end_date,
                adjust_type,
            ))
            .await?;
            left_bars.extend(right_bars);
            Ok(left_bars)
        }
        Err(err) => Err(err).with_context(|| {
            format!(
                "failed to fetch daily bars from qmt for batch size {}",
                symbols.len()
            )
        }),
    }
}
