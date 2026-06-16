//! 涨停横盘形态识别。
//!
//! 目标：
//! 识别个股在放量涨停后进入窄幅整理区，随后通过指标转强完成二次启动的形态。
//!
//! 当前实现：
//! 1. 在近 10 个交易日内查找涨停日，并要求当日成交量相对 5 日均量明显放大。
//! 2. 统计涨停日至今的横盘天数，要求横盘区间高低点受控，不能有效跌破涨停收盘支撑。
//! 3. 横盘期间至少出现缩量特征，表示抛压衰减。
//! 4. 最新日若出现 KDJ 金叉或 MACD 金叉，且价格重新走强，则输出横盘突破信号。

use serde_json::json;

use super::common::{is_bullish, latest_idx, pct_change, signal, window_high, window_low};
use super::PatternDetector;
use crate::patterns::indicators::SeriesIndicators;
use crate::patterns::model::{BarSeries, PatternSignal};

#[derive(Debug, Clone)]
pub struct LimitUpSidewaysDetector {
    pub limit_up_lookback_days: usize,
    pub limit_up_threshold: f64,
    pub volume_ratio_threshold: f64,
    pub sideways_days_min: usize,
    pub sideways_days_max: usize,
    pub sideways_high_limit: f64,
    pub sideways_low_limit: f64,
    pub volume_shrinkage_ratio: f64,
    pub support_drop_limit: f64,
    pub kdj_gold_cross_threshold: f64,
    pub volume_increase_ratio: f64,
}

impl Default for LimitUpSidewaysDetector {
    fn default() -> Self {
        Self {
            limit_up_lookback_days: 10,
            limit_up_threshold: 0.095,
            volume_ratio_threshold: 1.8,
            sideways_days_min: 1,
            sideways_days_max: 10,
            sideways_high_limit: 0.08,
            sideways_low_limit: -0.05,
            volume_shrinkage_ratio: 0.7,
            support_drop_limit: -0.01,
            kdj_gold_cross_threshold: 20.0,
            volume_increase_ratio: 1.3,
        }
    }
}

impl PatternDetector for LimitUpSidewaysDetector {
    fn id(&self) -> &'static str {
        "limit_up_sideways"
    }

    fn detect(&self, series: &BarSeries, indicators: &SeriesIndicators) -> Option<PatternSignal> {
        if series.len() < 15 {
            return None;
        }
        let latest_idx = latest_idx(series);

        for limit_idx in (latest_idx.saturating_sub(self.limit_up_lookback_days)..latest_idx).rev()
        {
            let change = pct_change(series, limit_idx)?;
            if change < self.limit_up_threshold {
                continue;
            }
            let limit_volume = series.volume_at(limit_idx)?;
            let limit_close = series.close_at(limit_idx)?;
            let limit_time = series.time_at(limit_idx)?;
            let vol_ma = indicators.volume_ma5[limit_idx]?;
            if limit_volume < vol_ma * self.volume_ratio_threshold {
                continue;
            }
            if latest_idx <= limit_idx {
                continue;
            }
            let sideways_days = latest_idx - limit_idx;
            if sideways_days < self.sideways_days_min || sideways_days > self.sideways_days_max {
                continue;
            }
            let highest = window_high(series, limit_idx + 1, latest_idx);
            let lowest = window_low(series, limit_idx + 1, latest_idx);
            if highest > limit_close * (1.0 + self.sideways_high_limit)
                || lowest < limit_close * (1.0 + self.sideways_low_limit)
            {
                continue;
            }
            let support_lower_limit = limit_close * (1.0 + self.support_drop_limit);
            if (limit_idx + 1..=latest_idx).any(|idx| {
                series
                    .close_at(idx)
                    .is_some_and(|close| close < support_lower_limit)
            }) {
                continue;
            }
            if (limit_idx + 1..=latest_idx).all(|idx| {
                series
                    .volume_at(idx)
                    .is_some_and(|volume| volume > limit_volume * self.volume_shrinkage_ratio)
            }) {
                continue;
            }
            if latest_idx == 0 {
                continue;
            }
            let latest_volume = series.volume_at(latest_idx)?;
            let latest_close = series.close_at(latest_idx)?;
            let latest_time = series.time_at(latest_idx)?;
            let prev_volume = series.volume_at(latest_idx - 1)?;
            let prev_close = series.close_at(latest_idx - 1)?;
            let volume_increase = latest_volume / prev_volume.max(1e-6);
            if latest_close <= prev_close || volume_increase < self.volume_increase_ratio {
                continue;
            }
            let k = indicators.k[latest_idx]?;
            let d = indicators.d[latest_idx]?;
            let prev_k = indicators.k[latest_idx - 1]?;
            let prev_d = indicators.d[latest_idx - 1]?;
            let dif = indicators.dif[latest_idx]?;
            let dea = indicators.dea[latest_idx]?;
            let prev_dif = indicators.dif[latest_idx - 1]?;
            let prev_dea = indicators.dea[latest_idx - 1]?;
            let kdj_cross = prev_k <= prev_d && k > d && k < self.kdj_gold_cross_threshold;
            let macd_cross = prev_dif <= prev_dea && dif > dea;
            if !is_bullish(series, latest_idx)? || !(kdj_cross || macd_cross) {
                continue;
            }
            let avg_sideways_volume = (limit_idx + 1..=latest_idx)
                .filter_map(|idx| series.volume_at(idx))
                .sum::<f64>()
                / sideways_days as f64;
            let price_change = (latest_close - prev_close) / prev_close.max(1e-6);
            let reasons = vec![
                format!(
                    "近 {} 日出现放量涨停，量比 {:.2}",
                    self.limit_up_lookback_days,
                    limit_volume / vol_ma.max(1e-6)
                ),
                format!(
                    "涨停后横盘 {} 天，区间 [{:.2}, {:.2}] 未跌破支撑 {:.2}",
                    sideways_days, lowest, highest, support_lower_limit
                ),
                format!(
                    "最新日成交量较前一日放大 {:.2} 倍，{}",
                    volume_increase,
                    if kdj_cross {
                        "出现 KDJ 金叉"
                    } else {
                        "出现 MACD 金叉"
                    }
                ),
            ];
            return Some(signal(
                self.id(),
                series,
                latest_time,
                0.76,
                &["limit-up", "sideways"],
                "近期涨停后横盘整理，最新一日以KDJ或MACD转强信号确认。",
                json!({
                    "key_date": limit_time.format("%Y-%m-%d").to_string(),
                    "key_date_type": "涨停日",
                    "limit_up_date": limit_time.format("%Y-%m-%d").to_string(),
                    "support_level": limit_close,
                    "sideways_days": sideways_days,
                    "sideways_highest": highest,
                    "sideways_lowest": lowest,
                    "sideways_volume_ratio": avg_sideways_volume / limit_volume.max(1e-6),
                    "support_lower_limit": support_lower_limit,
                    "volume_increase": volume_increase,
                    "price_change": price_change,
                    "kdj_cross": kdj_cross,
                    "macd_cross": macd_cross,
                    "reasons": reasons,
                }),
            ));
        }
        None
    }
}
