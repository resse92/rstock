use anyhow::{anyhow, Result};
use chrono::NaiveDate;
use polars::prelude::DataFrame;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::kline_frame::{daily_bars_to_frame, dedup_frame_by_symbol_time};
use crate::models::DailyBar;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Bar {
    pub symbol: String,
    pub exchange: String,
    pub time: NaiveDate,
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
    pub volume: f64,
    pub amount: Option<f64>,
    pub source: Option<String>,
}

impl Bar {
    pub fn from_daily_bar(bar: DailyBar) -> Result<Option<Self>> {
        let Some(open) = bar.open else {
            return Ok(None);
        };
        let Some(high) = bar.high else {
            return Ok(None);
        };
        let Some(low) = bar.low else {
            return Ok(None);
        };
        let Some(close) = bar.close else {
            return Ok(None);
        };
        let Some(volume) = bar.volume else {
            return Ok(None);
        };

        if !open.is_finite()
            || !high.is_finite()
            || !low.is_finite()
            || !close.is_finite()
            || !volume.is_finite()
            || volume < 0.0
        {
            return Ok(None);
        }

        let time = NaiveDate::parse_from_str(&bar.time, "%Y-%m-%d")
            .map_err(|err| anyhow!("invalid daily bar date {}: {err}", bar.time))?;

        Ok(Some(Self {
            symbol: bar.symbol,
            exchange: bar.exchange,
            time,
            open,
            high,
            low,
            close,
            volume,
            amount: bar.amount,
            source: bar.source,
        }))
    }
}

#[derive(Debug, Clone)]
pub struct BarSeries {
    pub symbol: String,
    pub exchange: String,
    pub frame: DataFrame,
}

impl BarSeries {
    pub fn new(symbol: String, exchange: String, mut bars: Vec<Bar>) -> Self {
        bars.sort_by_key(|bar| bar.time);
        bars.dedup_by_key(|bar| bar.time);
        let daily_bars = bars
            .iter()
            .map(|bar| DailyBar {
                symbol: bar.symbol.clone(),
                exchange: bar.exchange.clone(),
                time: bar.time.format("%Y-%m-%d").to_string(),
                open: Some(bar.open),
                high: Some(bar.high),
                low: Some(bar.low),
                close: Some(bar.close),
                volume: Some(bar.volume),
                amount: bar.amount,
                adj_factor: None,
                settle: None,
                open_interest: None,
                source: bar.source.clone(),
            })
            .collect::<Vec<_>>();
        let frame = daily_bars_to_frame(&daily_bars, "pattern")
            .ok()
            .and_then(|frame| dedup_frame_by_symbol_time(&frame).ok())
            .unwrap_or_default();
        Self {
            symbol,
            exchange,
            frame,
        }
    }

    pub fn from_frame(symbol: String, exchange: String, frame: DataFrame) -> Result<Self> {
        Ok(Self {
            symbol,
            exchange,
            frame: dedup_frame_by_symbol_time(&frame)?,
        })
    }

    pub fn len(&self) -> usize {
        self.frame.height()
    }

    pub fn is_empty(&self) -> bool {
        self.frame.height() == 0
    }

    pub fn time_at(&self, idx: usize) -> Option<NaiveDate> {
        if idx >= self.frame.height() {
            return None;
        }
        let time_raw = self.frame.column("time").ok()?.str().ok()?.get(idx)?;
        NaiveDate::parse_from_str(time_raw, "%Y-%m-%d").ok()
    }

    pub fn open_at(&self, idx: usize) -> Option<f64> {
        if idx >= self.frame.height() {
            return None;
        }
        self.frame.column("open").ok()?.f64().ok()?.get(idx)
    }

    pub fn high_at(&self, idx: usize) -> Option<f64> {
        if idx >= self.frame.height() {
            return None;
        }
        self.frame.column("high").ok()?.f64().ok()?.get(idx)
    }

    pub fn low_at(&self, idx: usize) -> Option<f64> {
        if idx >= self.frame.height() {
            return None;
        }
        self.frame.column("low").ok()?.f64().ok()?.get(idx)
    }

    pub fn close_at(&self, idx: usize) -> Option<f64> {
        if idx >= self.frame.height() {
            return None;
        }
        self.frame.column("close").ok()?.f64().ok()?.get(idx)
    }

    pub fn volume_at(&self, idx: usize) -> Option<f64> {
        if idx >= self.frame.height() {
            return None;
        }
        self.frame.column("volume").ok()?.f64().ok()?.get(idx)
    }

    pub fn amount_at(&self, idx: usize) -> Option<f64> {
        if idx >= self.frame.height() {
            return None;
        }
        self.frame.column("amount").ok()?.f64().ok()?.get(idx)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PatternSignal {
    pub pattern_id: String,
    pub symbol: String,
    pub exchange: String,
    pub signal_date: NaiveDate,
    pub score: f64,
    pub tags: Vec<String>,
    pub explanation: String,
    pub evidence: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PatternScanFailure {
    pub symbol: String,
    pub error: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PatternScanRequest {
    pub symbols: Vec<String>,
    pub start_date: NaiveDate,
    pub end_date: NaiveDate,
    pub refresh_remote: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PatternScanReport {
    pub requested_symbols: usize,
    pub skipped_short_series: usize,
    pub series_count: usize,
    pub signal_count: usize,
    pub fetched_symbols: Vec<String>,
    pub failed_symbols: Vec<PatternScanFailure>,
    pub signals: Vec<PatternSignal>,
}
