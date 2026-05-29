//! 阻力位突破形态识别。
//!
//! 目标：
//! 识别股价放量突破中期阻力位，并在突破后保持强势的趋势延续信号。
//!
//! 当前实现：
//! 1. 回看 60 根K线估算关键阻力位。
//! 2. 在最近 3 个交易日中寻找涨幅明显、成交量显著放大的突破阳线。
//! 3. 要求阻力高点与突破日之间有足够时间间隔，避免把近端震荡误判成突破。
//! 4. 突破后最低价不能有效跌回阻力位下方，同时均线保持多头排列。

use serde_json::json;

use super::common::{is_bullish, latest_idx, signal};
use super::PatternDetector;
use crate::patterns::indicators::SeriesIndicators;
use crate::patterns::model::{BarSeries, PatternSignal};

#[derive(Debug, Clone)]
pub struct ResistanceBreakoutDetector {
    pub breakout_lookback: usize,
    pub volume_ratio: f64,
}

impl Default for ResistanceBreakoutDetector {
    fn default() -> Self {
        Self {
            breakout_lookback: 60,
            volume_ratio: 2.2,
        }
    }
}

impl PatternDetector for ResistanceBreakoutDetector {
    fn id(&self) -> &'static str {
        "resistance_breakout"
    }

    fn detect(&self, series: &BarSeries, indicators: &SeriesIndicators) -> Option<PatternSignal> {
        let bars = &series.bars;
        if bars.len() <= self.breakout_lookback + 1 {
            return None;
        }

        let latest_idx = latest_idx(series);
        let latest = &bars[latest_idx];
        let ma5 = indicators.ma5[latest_idx]?;
        let ma10 = indicators.ma10[latest_idx]?;
        let ma20 = indicators.ma20[latest_idx]?;
        if !(ma5 > ma10 && ma10 > ma20) {
            return None;
        }

        let start_search = latest_idx.saturating_sub(3);
        for idx in (start_search..=latest_idx).rev() {
            let prev_close = if idx > 0 {
                bars[idx - 1].close
            } else {
                continue;
            };
            let change_pct = (bars[idx].close - prev_close) / prev_close.max(1e-6);
            let resistance_start = idx.saturating_sub(self.breakout_lookback);
            if idx <= resistance_start + 30 {
                continue;
            }
            let resistance = bars[resistance_start..idx]
                .iter()
                .map(|bar| bar.high)
                .fold(f64::NEG_INFINITY, f64::max);
            let vol_ma = indicators.volume_ma5[idx]?;
            if change_pct < 0.09
                || bars[idx].close < resistance
                || bars[idx].volume < vol_ma * self.volume_ratio
            {
                continue;
            }
            let max_before_idx = bars[resistance_start..idx]
                .iter()
                .enumerate()
                .max_by(|a, b| a.1.high.partial_cmp(&b.1.high).unwrap())
                .map(|(pos, _)| pos + resistance_start)?;
            if idx - max_before_idx < 30 {
                continue;
            }
            let support = resistance * 0.98;
            if bars[idx..=latest_idx]
                .iter()
                .map(|bar| bar.low)
                .fold(f64::INFINITY, f64::min)
                < support
            {
                continue;
            }
            if !is_bullish(&bars[idx]) || latest.close <= ma5 {
                continue;
            }
            return Some(signal(
                self.id(),
                series,
                latest.time,
                0.8,
                &["breakout", "trend"],
                "近阶段放量长阳突破阻力位，突破后回踩未失守。",
                json!({
                    "breakout_date": bars[idx].time.format("%Y-%m-%d").to_string(),
                    "resistance": resistance,
                    "support": support,
                    "breakout_change_pct": change_pct,
                    "volume_ratio": bars[idx].volume / vol_ma,
                }),
            ));
        }
        None
    }
}
