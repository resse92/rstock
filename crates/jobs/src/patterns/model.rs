use anyhow::{anyhow, Result};
use chrono::NaiveDate;
use serde::{Deserialize, Serialize};
use serde_json::Value;

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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BarSeries {
    pub symbol: String,
    pub exchange: String,
    pub bars: Vec<Bar>,
}

impl BarSeries {
    pub fn new(symbol: String, exchange: String, mut bars: Vec<Bar>) -> Self {
        bars.sort_by_key(|bar| bar.time);
        bars.dedup_by_key(|bar| bar.time);
        Self {
            symbol,
            exchange,
            bars,
        }
    }

    pub fn len(&self) -> usize {
        self.bars.len()
    }

    pub fn is_empty(&self) -> bool {
        self.bars.is_empty()
    }

    pub fn latest(&self) -> Option<&Bar> {
        self.bars.last()
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
    pub signals: Vec<PatternSignal>,
}
