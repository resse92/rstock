use chrono::NaiveDate;
use serde_json::{json, Value};

use crate::patterns::model::{BarSeries, PatternSignal};

pub fn signal(
    pattern_id: &str,
    series: &BarSeries,
    signal_date: NaiveDate,
    score: f64,
    tags: &[&str],
    explanation: impl Into<String>,
    evidence: Value,
) -> PatternSignal {
    PatternSignal {
        pattern_id: pattern_id.to_string(),
        symbol: series.symbol.clone(),
        exchange: series.exchange.clone(),
        signal_date,
        score,
        tags: tags.iter().map(|item| item.to_string()).collect(),
        explanation: explanation.into(),
        evidence,
    }
}

pub fn pct_change(series: &BarSeries, idx: usize) -> Option<f64> {
    if idx == 0 || idx >= series.len() {
        return None;
    }
    let prev = series.close_at(idx - 1)?;
    if prev <= 0.0 {
        return None;
    }
    Some((series.close_at(idx)? - prev) / prev)
}

pub fn body_ratio(series: &BarSeries, idx: usize) -> Option<f64> {
    let open = series.open_at(idx)?;
    let close = series.close_at(idx)?;
    Some(if open.abs() < f64::EPSILON {
        0.0
    } else {
        (close - open).abs() / open
    })
}

pub fn is_bullish(series: &BarSeries, idx: usize) -> Option<bool> {
    Some(series.close_at(idx)? > series.open_at(idx)?)
}

pub fn is_bearish(series: &BarSeries, idx: usize) -> Option<bool> {
    Some(series.close_at(idx)? < series.open_at(idx)?)
}

pub fn window_high(series: &BarSeries, start: usize, end_inclusive: usize) -> f64 {
    let mut out = f64::NEG_INFINITY;
    for idx in start..=end_inclusive {
        if let Some(high) = series.high_at(idx) {
            out = out.max(high);
        }
    }
    out
}

pub fn window_low(series: &BarSeries, start: usize, end_inclusive: usize) -> f64 {
    let mut out = f64::INFINITY;
    for idx in start..=end_inclusive {
        if let Some(low) = series.low_at(idx) {
            out = out.min(low);
        }
    }
    out
}

pub fn latest_idx(series: &BarSeries) -> usize {
    series.len().saturating_sub(1)
}

pub fn linear_regression_metrics(values: &[f64]) -> Option<(f64, f64)> {
    if values.len() < 3 {
        return None;
    }
    let n = values.len() as f64;
    let x_mean = (n - 1.0) / 2.0;
    let y_mean = values.iter().sum::<f64>() / n;
    let mut numerator = 0.0;
    let mut denominator = 0.0;
    for (idx, value) in values.iter().enumerate() {
        let x = idx as f64;
        numerator += (x - x_mean) * (value - y_mean);
        denominator += (x - x_mean).powi(2);
    }
    if denominator.abs() < f64::EPSILON {
        return None;
    }
    let slope = numerator / denominator;
    let intercept = y_mean - slope * x_mean;
    let ss_tot = values
        .iter()
        .map(|value| (value - y_mean).powi(2))
        .sum::<f64>();
    if ss_tot.abs() < f64::EPSILON {
        return Some((slope, 1.0));
    }
    let ss_res = values
        .iter()
        .enumerate()
        .map(|(idx, value)| {
            let pred = intercept + slope * idx as f64;
            (value - pred).powi(2)
        })
        .sum::<f64>();
    let r2 = 1.0 - ss_res / ss_tot;
    Some((slope, r2.max(0.0)))
}

pub fn as_json_date(date: NaiveDate) -> Value {
    json!(date.format("%Y-%m-%d").to_string())
}
