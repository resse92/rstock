//! 涨停回马枪形态识别。
//!
//! 目标：
//! 识别个股在近期放量涨停后，经历短暂健康回调并重新转强的形态。
//!
//! 当前实现：
//! 1. 在最近若干交易日内寻找涨幅接近涨停阈值的 K 线。
//! 2. 要求该涨停日成交量明显高于此前 5 日基准均量。
//! 3. 从涨停日到最新日之间，回调天数受限，最低价不能跌破涨停收盘价的关键支撑。
//! 4. 回调区间振幅不能过大，且最新一日必须重新收阳，视为“回马枪”成立。

use serde_json::json;

use super::common::{is_bullish, latest_idx, pct_change, signal, window_high, window_low};
use super::PatternDetector;
use crate::patterns::indicators::SeriesIndicators;
use crate::patterns::model::{BarSeries, PatternSignal};

#[derive(Debug, Clone)]
pub struct LimitUpPullbackDetector {
    pub lookback_days: usize,
    pub limit_up_threshold: f64,
    pub volume_ratio_threshold: f64,
    pub min_pullback_days: usize,
    pub max_pullback_days: usize,
    pub support_ratio: f64,
    pub resistance_ratio: f64,
    pub volume_shrinkage_ratio: f64,
}

impl Default for LimitUpPullbackDetector {
    fn default() -> Self {
        Self {
            lookback_days: 6,
            limit_up_threshold: 0.095,
            volume_ratio_threshold: 2.2,
            min_pullback_days: 1,
            max_pullback_days: 9,
            support_ratio: 0.95,
            resistance_ratio: 1.05,
            volume_shrinkage_ratio: 0.5,
        }
    }
}

impl PatternDetector for LimitUpPullbackDetector {
    fn id(&self) -> &'static str {
        "limit_up_pullback"
    }

    fn detect(&self, series: &BarSeries, indicators: &SeriesIndicators) -> Option<PatternSignal> {
        if series.len() < 12 {
            return None;
        }

        let latest_idx = latest_idx(series);
        let search_start = latest_idx.saturating_sub(self.lookback_days + self.max_pullback_days);
        let search_end = latest_idx.saturating_sub(1);

        for limit_idx in (search_start..=search_end).rev() {
            let day_change = pct_change(series, limit_idx)?;
            if day_change < self.limit_up_threshold {
                continue;
            }

            let limit_close = series.close_at(limit_idx)?;
            let limit_volume = series.volume_at(limit_idx)?;
            let limit_time = series.time_at(limit_idx)?;

            let volume_start = limit_idx.saturating_sub(5);
            let mut baseline_volume = 0.0;
            for idx in volume_start..limit_idx {
                baseline_volume += series.volume_at(idx)?;
            }
            baseline_volume /= (limit_idx - volume_start).max(1) as f64;
            if baseline_volume <= 0.0
                || limit_volume / baseline_volume < self.volume_ratio_threshold
            {
                continue;
            }

            let pullback_days = latest_idx - limit_idx;
            if pullback_days < self.min_pullback_days || pullback_days > self.max_pullback_days {
                continue;
            }

            let pullback_start = limit_idx + 1;
            let lowest_low = window_low(series, pullback_start, latest_idx);
            if lowest_low < limit_close * self.support_ratio {
                continue;
            }

            let highest_high = window_high(series, pullback_start, latest_idx);
            let pullback_range = (highest_high - lowest_low) / highest_high.max(1e-6);
            if pullback_range > 0.15 {
                continue;
            }

            let pullback_closes = (pullback_start..=latest_idx)
                .filter_map(|idx| series.close_at(idx))
                .collect::<Vec<_>>();
            if pullback_closes
                .iter()
                .any(|close| *close < limit_close * self.support_ratio)
            {
                continue;
            }
            if pullback_closes
                .iter()
                .any(|close| *close > limit_close * self.resistance_ratio)
            {
                continue;
            }
            if pullback_closes.iter().all(|close| *close >= limit_close) {
                continue;
            }
            let shrink_threshold = limit_volume * self.volume_shrinkage_ratio;
            if (pullback_start..=latest_idx).all(|idx| {
                series
                    .volume_at(idx)
                    .is_some_and(|volume| volume > shrink_threshold)
            }) {
                continue;
            }
            let latest_time = series.time_at(latest_idx)?;
            let latest_volume = series.volume_at(latest_idx)?;
            if !is_bullish(series, latest_idx)? {
                continue;
            }

            let support_price = limit_close * self.support_ratio;
            let resistance_price = limit_close * self.resistance_ratio;
            let has_lower_close = pullback_closes.iter().any(|close| *close < limit_close);
            let has_volume_shrinkage = (pullback_start..=latest_idx).any(|idx| {
                series
                    .volume_at(idx)
                    .is_some_and(|volume| volume <= shrink_threshold)
            });

            let vol_ratio = indicators.volume_ma5[latest_idx]
                .map(|value| latest_volume / value.max(1e-6))
                .unwrap_or(0.0);

            return Some(signal(
                self.id(),
                series,
                latest_time,
                0.82,
                &["price-action", "volume-confirmed"],
                "最近出现放量涨停，随后回调未破关键支撑，最新一日转强。",
                json!({
                    "key_date": limit_time.format("%Y-%m-%d").to_string(),
                    "key_date_type": "涨停日",
                    "limit_up_date": limit_time.format("%Y-%m-%d").to_string(),
                    "pullback_days": pullback_days,
                    "pullback_range": pullback_range,
                    "volume_ratio": limit_volume / baseline_volume,
                    "support_price": support_price,
                    "resistance_price": resistance_price,
                    "has_lower_close": has_lower_close,
                    "has_volume_shrinkage": has_volume_shrinkage,
                    "latest_volume_ratio": vol_ratio,
                    "reasons": [
                        format!("涨停日量比 {:.2}", limit_volume / baseline_volume),
                        format!("回调 {} 天，区间振幅 {:.2}%", pullback_days, pullback_range * 100.0),
                        format!("回调期间未破支撑 {:.2}，且至少出现一次缩量", support_price),
                    ],
                }),
            ));
        }
        None
    }
}
