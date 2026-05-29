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

#[derive(Debug, Clone)]
pub struct PatternDataSourceConfig {
    pub base_url: String,
    pub authorization: Option<String>,
    pub timeout_secs: u64,
    pub adjust_type: String,
    pub tdx_fallback: bool,
}

impl PatternDataSourceConfig {
    pub fn new(base_url: impl Into<String>) -> Self {
        Self {
            base_url: base_url.into(),
            authorization: None,
            timeout_secs: 30,
            adjust_type: "qfq".to_string(),
            tdx_fallback: true,
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

        let refreshed_symbols = if request.refresh_remote {
            self.refresh_cache(&request.symbols, request.start_date, request.end_date)
                .await?
        } else {
            self.refresh_stale_cache(&request.symbols, request.start_date, request.end_date)
                .await?
        };

        let series =
            self.cache
                .load_series(&request.symbols, request.start_date, request.end_date)?;
        let mut signals = Vec::<PatternSignal>::new();

        for item in &series {
            if item.len() < 20 {
                continue;
            }
            let indicators = SeriesIndicators::calculate(item);
            for detector in &self.detectors {
                if let Some(signal) = detector.detect(item, &indicators) {
                    signals.push(signal);
                }
            }
        }

        Ok(PatternScanReport {
            series_count: series.len(),
            signal_count: signals.len(),
            refreshed_symbols,
            signals,
        })
    }

    async fn refresh_stale_cache(
        &self,
        symbols: &[String],
        start_date: NaiveDate,
        end_date: NaiveDate,
    ) -> Result<Vec<String>> {
        let latest = self.cache.latest_dates(symbols)?;
        let stale_symbols: Vec<String> = symbols
            .iter()
            .filter(|symbol| {
                latest
                    .get(*symbol)
                    .map(|date| *date < end_date)
                    .unwrap_or(true)
            })
            .cloned()
            .collect();

        if stale_symbols.is_empty() {
            return Ok(Vec::new());
        }

        self.refresh_cache(&stale_symbols, start_date, end_date)
            .await
    }

    async fn refresh_cache(
        &self,
        symbols: &[String],
        start_date: NaiveDate,
        end_date: NaiveDate,
    ) -> Result<Vec<String>> {
        let bars = self
            .fetch_remote_bars(symbols, start_date, end_date)
            .await?;
        self.cache.upsert_daily_bars(&bars)?;
        Ok(symbols.to_vec())
    }

    async fn fetch_remote_bars(
        &self,
        symbols: &[String],
        start_date: NaiveDate,
        end_date: NaiveDate,
    ) -> Result<Vec<Bar>> {
        let api = ApiClient::new(
            self.data_source.base_url.clone(),
            self.data_source.authorization.clone(),
            Duration::from_secs(self.data_source.timeout_secs),
        )?;

        let request = MarketRequest::new(
            symbols.to_vec(),
            "1d",
            compact_date(start_date),
            compact_date(end_date),
            self.data_source.adjust_type.clone(),
        );

        let payload = api
            .fetch_market_batch(&request)
            .await
            .context("failed to fetch daily bars from qmt")?;
        let mut daily_bars = normalize_full_kline_response(&payload, "1d")
            .into_iter()
            .filter_map(|row| DailyBar::from_normalized(&row))
            .collect::<Vec<_>>();
        for bar in &mut daily_bars {
            bar.source = Some("qmt".to_string());
        }

        let fetched_symbols: HashSet<String> =
            daily_bars.iter().map(|bar| bar.symbol.clone()).collect();
        if self.data_source.tdx_fallback {
            let missing: Vec<String> = symbols
                .iter()
                .filter(|symbol| !fetched_symbols.contains(*symbol))
                .cloned()
                .collect();
            if !missing.is_empty() {
                let fallback = tdx_source::fetch_daily_bars(
                    &missing,
                    &compact_date(start_date),
                    &compact_date(end_date),
                )?;
                daily_bars.extend(fallback);
            }
        }

        let mut bars = Vec::new();
        for daily_bar in daily_bars {
            if let Some(bar) = Bar::from_daily_bar(daily_bar)? {
                bars.push(bar);
            }
        }
        Ok(bars)
    }
}

fn compact_date(date: NaiveDate) -> String {
    date.format("%Y%m%d").to_string()
}
