use chrono::NaiveDate;
use polars::prelude::DataFrame;
use serde_json::{json, Value};

use crate::patterns::indicators::{
    boll_lower_column_name, boll_mid_column_name, boll_upper_column_name,
    bull_bear_line_column_name, d_column_name, j_column_name, k_column_name, ma_column_name,
    macd_dea_column_name, macd_dif_column_name, macd_hist_column_name, rsi_column_name,
    short_trend_column_name, volume_ma_column_name,
};
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

pub fn indicator_value(indicators: &DataFrame, column: &str, idx: usize) -> Option<f64> {
    indicators.column(column).ok()?.f64().ok()?.get(idx)
}

pub fn ma(indicators: &DataFrame, idx: usize, period: usize) -> Option<f64> {
    indicator_value(indicators, &ma_column_name(period), idx)
}

pub fn volume_ma(indicators: &DataFrame, idx: usize, period: usize) -> Option<f64> {
    indicator_value(indicators, &volume_ma_column_name(period), idx)
}

pub fn macd_dif(indicators: &DataFrame, idx: usize, fast: usize, slow: usize) -> Option<f64> {
    indicator_value(indicators, &macd_dif_column_name(fast, slow), idx)
}

pub fn macd_dea(
    indicators: &DataFrame,
    idx: usize,
    fast: usize,
    slow: usize,
    signal: usize,
) -> Option<f64> {
    indicator_value(indicators, &macd_dea_column_name(fast, slow, signal), idx)
}

pub fn macd_hist(
    indicators: &DataFrame,
    idx: usize,
    fast: usize,
    slow: usize,
    signal: usize,
) -> Option<f64> {
    indicator_value(indicators, &macd_hist_column_name(fast, slow, signal), idx)
}

pub fn kdj_k(
    indicators: &DataFrame,
    idx: usize,
    n: usize,
    k_period: usize,
    d_period: usize,
) -> Option<f64> {
    indicator_value(indicators, &k_column_name(n, k_period, d_period), idx)
}

pub fn kdj_d(
    indicators: &DataFrame,
    idx: usize,
    n: usize,
    k_period: usize,
    d_period: usize,
) -> Option<f64> {
    indicator_value(indicators, &d_column_name(n, k_period, d_period), idx)
}

pub fn kdj_j(
    indicators: &DataFrame,
    idx: usize,
    n: usize,
    k_period: usize,
    d_period: usize,
) -> Option<f64> {
    indicator_value(indicators, &j_column_name(n, k_period, d_period), idx)
}

pub fn boll_mid(indicators: &DataFrame, idx: usize, period: usize) -> Option<f64> {
    indicator_value(indicators, &boll_mid_column_name(period), idx)
}

pub fn boll_upper(indicators: &DataFrame, idx: usize, period: usize, std_dev: f64) -> Option<f64> {
    indicator_value(indicators, &boll_upper_column_name(period, std_dev), idx)
}

pub fn boll_lower(indicators: &DataFrame, idx: usize, period: usize, std_dev: f64) -> Option<f64> {
    indicator_value(indicators, &boll_lower_column_name(period, std_dev), idx)
}

pub fn rsi(indicators: &DataFrame, idx: usize, period: usize) -> Option<f64> {
    indicator_value(indicators, &rsi_column_name(period), idx)
}

pub fn short_trend(indicators: &DataFrame, idx: usize, fast: usize, smooth: usize) -> Option<f64> {
    indicator_value(indicators, &short_trend_column_name(fast, smooth), idx)
}

pub fn bull_bear_line(indicators: &DataFrame, idx: usize, periods: [usize; 4]) -> Option<f64> {
    indicator_value(indicators, &bull_bear_line_column_name(periods), idx)
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
