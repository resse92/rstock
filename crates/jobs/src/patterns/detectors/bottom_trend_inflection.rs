//! 底部趋势拐点形态识别。
//!
//! 目标：
//! 识别经历中期深跌后，在底部区域出现放量反弹并伴随动能改善的反转信号。
//!
//! 当前实现：
//! 1. 回看 120 根K线，要求从阶段高点到后续低点的跌幅达到 45%。
//! 2. 最新交易日涨幅达到 8%，且成交量至少是 10 日均量的 2.5 倍。
//! 3. 最近 20 根K线里，价格低点线性回归斜率仍偏弱，但 MACD 柱动能斜率相对改善，
//!    作为“底背离/跌势衰减”的近似判定。
//! 4. 满足上述条件后输出底部反转信号。

use serde_json::json;

use super::common::{linear_regression_metrics, pct_change, signal};
use super::PatternDetector;
use crate::patterns::indicators::SeriesIndicators;
use crate::patterns::model::{BarSeries, PatternSignal};

#[derive(Debug, Clone, Default)]
pub struct BottomTrendInflectionDetector;

impl PatternDetector for BottomTrendInflectionDetector {
    fn id(&self) -> &'static str {
        "bottom_trend_inflection"
    }

    fn detect(&self, series: &BarSeries, indicators: &SeriesIndicators) -> Option<PatternSignal> {
        let bars = &series.bars;
        if bars.len() < 120 {
            return None;
        }
        let end = bars.len();
        let start = end - 120;
        let highest = bars[start..end]
            .iter()
            .map(|bar| bar.high)
            .fold(f64::NEG_INFINITY, f64::max);
        let highest_idx = bars[start..end]
            .iter()
            .enumerate()
            .max_by(|a, b| a.1.high.partial_cmp(&b.1.high).unwrap())
            .map(|(idx, _)| idx + start)?;
        let lowest_after_high = bars[highest_idx..end]
            .iter()
            .map(|bar| bar.low)
            .fold(f64::INFINITY, f64::min);
        let decline = (highest - lowest_after_high) / highest.max(1e-6);
        if decline < 0.45 {
            return None;
        }
        let recent_slice = end.saturating_sub(20);
        let recent_lows: Vec<f64> = bars[recent_slice..end].iter().map(|bar| bar.low).collect();
        let recent_macd: Vec<f64> = indicators.macd_hist[recent_slice..end]
            .iter()
            .map(|value| value.unwrap_or(0.0))
            .collect();
        let price_reg = linear_regression_metrics(&recent_lows)?;
        let macd_reg = linear_regression_metrics(&recent_macd)?;
        if !(price_reg.0 < 0.0 && macd_reg.0 > price_reg.0) {
            return None;
        }

        let surge_window_start = end.saturating_sub(10);
        for surge_idx in (surge_window_start..end).rev() {
            let day_change = pct_change(bars, surge_idx)?;
            let vol_ma10 = indicators.volume_ma10[surge_idx]?;
            if day_change <= 0.08 || bars[surge_idx].volume < vol_ma10 * 2.5 {
                continue;
            }

            let lowest_price = bars[start..end]
                .iter()
                .map(|bar| bar.low)
                .fold(f64::INFINITY, f64::min);
            let distance_ratio = (bars[surge_idx].close - lowest_price) / lowest_price.max(1e-6);
            if distance_ratio > 0.15 {
                continue;
            }

            let support_price = bars[surge_idx].open;
            let after_surge_start = surge_idx.saturating_add(1);
            if after_surge_start < end
                && bars[after_surge_start..end]
                    .iter()
                    .any(|bar| bar.low < support_price)
            {
                continue;
            }

            return Some(signal(
                self.id(),
                series,
                bars[end - 1].time,
                0.78,
                &["bottom", "reversal"],
                "半年深跌后出现放量反弹，价格创新低力度减弱且MACD出现底背离迹象。",
                json!({
                    "decline_ratio": decline,
                    "surge_date": bars[surge_idx].time.format("%Y-%m-%d").to_string(),
                    "surge_change_pct": day_change,
                    "volume_ratio": bars[surge_idx].volume / vol_ma10,
                    "distance_from_low": distance_ratio,
                    "support_price": support_price,
                    "price_slope": price_reg.0,
                    "macd_slope": macd_reg.0,
                }),
            ));
        }

        None
    }
}
