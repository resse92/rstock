use std::collections::HashSet;
use std::time::Duration;

use anyhow::{anyhow, Context, Result};
use chrono::NaiveDate;

use crate::api::ApiClient;
use crate::models::{DailyBar, MarketRequest};
use crate::normalize::normalize_full_kline_response;
use crate::patterns::cache::DuckDbPatternCache;
use crate::patterns::detectors::PatternDetector;
use crate::patterns::indicators::SeriesIndicators;
use crate::patterns::model::{
    Bar, PatternCacheConfig, PatternScanReport, PatternScanRequest, PatternSignal,
};
use crate::tdx_source;
use crate::utils::chunked;
use tracing::info;

#[derive(Debug, Clone)]
pub struct PatternDataSourceConfig {
    pub base_url: String,
    pub authorization: Option<String>,
    pub timeout_secs: u64,
    pub adjust_type: String,
    pub tdx_fallback: bool,
    pub batch_size: usize,
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
        }
    }
}

pub struct PatternScanner {
    cache: DuckDbPatternCache,
    detectors: Vec<Box<dyn PatternDetector>>,
    data_source: PatternDataSourceConfig,
}

impl PatternScanner {
    pub fn new(
        cache_config: PatternCacheConfig,
        data_source: PatternDataSourceConfig,
        detectors: Vec<Box<dyn PatternDetector>>,
    ) -> Result<Self> {
        Ok(Self {
            cache: DuckDbPatternCache::new(cache_config)?,
            detectors,
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
        let refreshed_symbols = self
            .refresh_missing_cache(
                &request.symbols,
                request.start_date,
                request.end_date,
                request.refresh_remote,
            )
            .await?;
        let series =
            self.cache
                .load_series(&request.symbols, request.start_date, request.end_date)?;

        let mut signals = Vec::<PatternSignal>::new();
        let series_count = series.len();
        for item in &series {
            if item.len() < 20 {
                info!(
                    target: "rstock_jobs::patterns",
                    symbol = %item.symbol,
                    bars = item.len(),
                    "skipped insufficient bars"
                );
                continue;
            }
            let indicators = SeriesIndicators::calculate(&item);
            let before = signals.len();
            for detector in &self.detectors {
                if let Some(signal) = detector.detect(&item, &indicators) {
                    signals.push(signal);
                }
            }
            let matched = signals.len() - before;
            info!(
                target: "rstock_jobs::patterns",
                symbol = %item.symbol,
                bars = item.len(),
                matched,
                "pattern scan symbol complete"
            );
        }

        info!(
            target: "rstock_jobs::patterns",
            series_count,
            signal_count = signals.len(),
            refreshed_symbols = refreshed_symbols.len(),
            "pattern scan done"
        );
        Ok(PatternScanReport {
            series_count,
            signal_count: signals.len(),
            refreshed_symbols,
            signals,
        })
    }

    async fn refresh_missing_cache(
        &self,
        symbols: &[String],
        start_date: NaiveDate,
        end_date: NaiveDate,
        force_refresh: bool,
    ) -> Result<Vec<String>> {
        let stale_symbols = if force_refresh {
            symbols.to_vec()
        } else {
            let summaries = self
                .cache
                .series_range_summaries(symbols, start_date, end_date)?;
            let mut stale = Vec::new();
            for symbol in symbols {
                let Some(summary) = summaries.get(symbol) else {
                    info!(
                        target: "rstock_jobs::patterns",
                        symbol = %symbol,
                        "series cache missing summary"
                    );
                    stale.push(symbol.clone());
                    continue;
                };
                let needs_refresh = summary.bar_count == 0
                    || summary.min_date > start_date
                    || summary.max_date < end_date;
                info!(
                    target: "rstock_jobs::patterns",
                    symbol = %summary.symbol,
                    cached_bars = summary.bar_count,
                    min_date = %summary.min_date,
                    max_date = %summary.max_date,
                    needs_refresh,
                    "series range summary"
                );
                if needs_refresh {
                    stale.push(symbol.clone());
                }
            }
            stale
        };

        if stale_symbols.is_empty() {
            info!(
                target: "rstock_jobs::patterns",
                "all requested symbols satisfied by cache"
            );
            return Ok(Vec::new());
        }

        info!(
            target: "rstock_jobs::patterns",
            stale_symbols = stale_symbols.len(),
            force_refresh,
            "refreshing stale symbols"
        );
        let bars = self
            .fetch_remote_bars(&stale_symbols, start_date, end_date)
            .await?;
        self.cache.upsert_daily_bars(&bars)?;
        info!(
            target: "rstock_jobs::patterns",
            stale_symbols = stale_symbols.len(),
            upsert_bars = bars.len(),
            "stale symbols refreshed"
        );
        Ok(stale_symbols)
    }

    async fn fetch_remote_bars(
        &self,
        symbols: &[String],
        start_date: NaiveDate,
        end_date: NaiveDate,
    ) -> Result<Vec<Bar>> {
        if symbols.is_empty() {
            return Ok(Vec::new());
        }

        let api = ApiClient::new(
            self.data_source.base_url.clone(),
            self.data_source.authorization.clone(),
            Duration::from_secs(self.data_source.timeout_secs),
        )?;
        let batch_size = self.data_source.batch_size.max(1);
        let symbol_batches = chunked(symbols, batch_size);
        let start = compact_date(start_date);
        let end = compact_date(end_date);
        let mut all_daily_bars = Vec::new();

        for (batch_idx, batch) in symbol_batches.into_iter().enumerate() {
            info!(
                target: "rstock_jobs::patterns",
                batch = batch_idx + 1,
                size = batch.len(),
                start_date = %start,
                end_date = %end,
                "fetch remote bars batch"
            );
            let request = MarketRequest::new(
                batch.clone(),
                "1d",
                start.clone(),
                end.clone(),
                self.data_source.adjust_type.clone(),
            );

            let payload = api.fetch_market_batch(&request).await.with_context(|| {
                format!(
                    "failed to fetch daily bars from qmt for batch size {}",
                    batch.len()
                )
            })?;
            let mut daily_bars = normalize_full_kline_response(&payload, "1d")
                .into_iter()
                .filter_map(|row| DailyBar::from_normalized(&row))
                .collect::<Vec<_>>();
            for bar in &mut daily_bars {
                bar.source = Some("qmt".to_string());
            }
            info!(
                target: "rstock_jobs::patterns",
                batch = batch_idx + 1,
                qmt_daily_bars = daily_bars.len(),
                "qmt batch fetched"
            );

            let fetched_symbols: HashSet<String> =
                daily_bars.iter().map(|bar| bar.symbol.clone()).collect();
            if self.data_source.tdx_fallback {
                let missing: Vec<String> = batch
                    .iter()
                    .filter(|symbol| !fetched_symbols.contains(*symbol))
                    .cloned()
                    .collect();
                if !missing.is_empty() {
                    info!(
                        target: "rstock_jobs::patterns",
                        batch = batch_idx + 1,
                        qmt_missing_symbols = missing.len(),
                        "qmt missing symbols, fallback to tdx"
                    );
                    let fallback = tdx_source::fetch_daily_bars(&missing, &start, &end)?;
                    info!(
                        target: "rstock_jobs::patterns",
                        batch = batch_idx + 1,
                        tdx_daily_bars = fallback.len(),
                        "tdx fallback fetched"
                    );
                    daily_bars.extend(fallback);
                }
            }

            all_daily_bars.extend(daily_bars);
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
}

fn compact_date(date: NaiveDate) -> String {
    date.format("%Y%m%d").to_string()
}
