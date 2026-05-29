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
        let bars = &series.bars;
        if bars.len() < 12 {
            return None;
        }

        let latest_idx = latest_idx(series);
        let search_start = latest_idx.saturating_sub(self.lookback_days + self.max_pullback_days);
        let search_end = latest_idx.saturating_sub(1);

        for limit_idx in (search_start..=search_end).rev() {
            let day_change = pct_change(bars, limit_idx)?;
            if day_change < self.limit_up_threshold {
                continue;
            }

            let volume_start = limit_idx.saturating_sub(5);
            let baseline_volume = bars[volume_start..limit_idx]
                .iter()
                .map(|bar| bar.volume)
                .sum::<f64>()
                / (limit_idx - volume_start).max(1) as f64;
            if baseline_volume <= 0.0
                || bars[limit_idx].volume / baseline_volume < self.volume_ratio_threshold
            {
                continue;
            }

            let pullback_days = latest_idx - limit_idx;
            if pullback_days < self.min_pullback_days || pullback_days > self.max_pullback_days {
                continue;
            }

            let pullback_start = limit_idx + 1;
            let lowest_low = window_low(bars, pullback_start, latest_idx);
            if lowest_low < bars[limit_idx].close * self.support_ratio {
                continue;
            }

            let highest_high = window_high(bars, pullback_start, latest_idx);
            let pullback_range = (highest_high - lowest_low) / highest_high.max(1e-6);
            if pullback_range > 0.15 {
                continue;
            }

            let pullback_closes: Vec<f64> = bars[pullback_start..=latest_idx]
                .iter()
                .map(|bar| bar.close)
                .collect();
            if pullback_closes
                .iter()
                .any(|close| *close < bars[limit_idx].close * self.support_ratio)
            {
                continue;
            }
            if pullback_closes
                .iter()
                .any(|close| *close > bars[limit_idx].close * self.resistance_ratio)
            {
                continue;
            }
            if pullback_closes
                .iter()
                .all(|close| *close >= bars[limit_idx].close)
            {
                continue;
            }
            let shrink_threshold = bars[limit_idx].volume * self.volume_shrinkage_ratio;
            if bars[pullback_start..=latest_idx]
                .iter()
                .all(|bar| bar.volume > shrink_threshold)
            {
                continue;
            }
            if !is_bullish(&bars[latest_idx]) {
                continue;
            }

            let vol_ratio = indicators
                .volume_ma5
                .get(latest_idx)
                .and_then(|value| *value)
                .map(|value| bars[latest_idx].volume / value.max(1e-6))
                .unwrap_or(0.0);

            return Some(signal(
                self.id(),
                series,
                bars[latest_idx].time,
                0.82,
                &["price-action", "volume-confirmed"],
                "最近出现放量涨停，随后回调未破关键支撑，最新一日转强。",
                json!({
                    "limit_up_date": bars[limit_idx].time.format("%Y-%m-%d").to_string(),
                    "pullback_days": pullback_days,
                    "pullback_range": pullback_range,
                    "volume_ratio": bars[limit_idx].volume / baseline_volume,
                    "support_price": bars[limit_idx].close * self.support_ratio,
                    "latest_volume_ratio": vol_ratio,
                }),
            ));
        }
        None
    }
}
