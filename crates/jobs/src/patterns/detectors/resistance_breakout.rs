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
    pub breakout_ratio: f64,
    pub min_change_pct: f64,
    pub volume_ratio: f64,
    pub volume_ma_period: usize,
    pub max_search_days: usize,
    pub min_resistance_gap: usize,
}

impl Default for ResistanceBreakoutDetector {
    fn default() -> Self {
        Self {
            breakout_lookback: 60,
            breakout_ratio: 0.0,
            min_change_pct: 0.09,
            volume_ratio: 2.2,
            volume_ma_period: 5,
            max_search_days: 3,
            min_resistance_gap: 30,
        }
    }
}

impl PatternDetector for ResistanceBreakoutDetector {
    fn id(&self) -> &'static str {
        "resistance_breakout"
    }

    fn detect(&self, series: &BarSeries, indicators: &SeriesIndicators) -> Option<PatternSignal> {
        if series.len() <= self.breakout_lookback + 1 {
            return None;
        }

        let latest_idx = latest_idx(series);
        let latest = series.bar(latest_idx)?;
        let ma5 = indicators.ma5[latest_idx]?;
        let ma10 = indicators.ma10[latest_idx]?;
        let ma20 = indicators.ma20[latest_idx]?;
        if !(ma5 > ma10 && ma10 > ma20) {
            return None;
        }

        let start_search = latest_idx.saturating_sub(self.max_search_days);
        for idx in (start_search..=latest_idx).rev() {
            let prev_close = if idx > 0 {
                series.bar(idx - 1)?.close
            } else {
                continue;
            };
            let current = series.bar(idx)?;
            let change_pct = (current.close - prev_close) / prev_close.max(1e-6);
            let resistance_start = idx.saturating_sub(self.breakout_lookback);
            if idx <= resistance_start + self.min_resistance_gap {
                continue;
            }
            let mut resistance = f64::NEG_INFINITY;
            for probe in resistance_start..idx {
                resistance = resistance.max(series.bar(probe)?.high);
            }
            let vol_ma = match self.volume_ma_period {
                5 => indicators.volume_ma5[idx],
                10 => indicators.volume_ma10[idx],
                60 => indicators.volume_ma60[idx],
                _ => indicators.volume_ma5[idx],
            }?;
            if change_pct < self.min_change_pct
                || current.close < resistance * (1.0 + self.breakout_ratio)
                || current.volume < vol_ma * self.volume_ratio
            {
                continue;
            }
            let mut max_before_idx = None;
            let mut max_before_high = f64::NEG_INFINITY;
            for probe in resistance_start..idx {
                let high = series.bar(probe)?.high;
                if high > max_before_high {
                    max_before_high = high;
                    max_before_idx = Some(probe);
                }
            }
            let max_before_idx = max_before_idx?;
            if idx - max_before_idx < self.min_resistance_gap {
                continue;
            }
            let support = resistance * 0.98;
            let mut min_low_after_breakout = f64::INFINITY;
            let mut has_hold = false;
            for probe in idx.saturating_add(1)..=latest_idx {
                has_hold = true;
                min_low_after_breakout = min_low_after_breakout.min(series.bar(probe)?.low);
            }
            if has_hold && min_low_after_breakout < support {
                continue;
            }
            if !is_bullish(&current) || latest.close <= ma5 {
                continue;
            }
            let days_since_breakout = latest_idx - idx;
            let volume_expand = current.volume / vol_ma.max(1e-6);
            let reasons = if days_since_breakout > 0 {
                vec![
                    format!(
                        "放量长阳突破{}日阻力位 {:.2}，涨幅 {:.1}%",
                        self.breakout_lookback,
                        resistance,
                        change_pct * 100.0
                    ),
                    format!("突破日成交量放大 {:.1} 倍", volume_expand),
                    format!(
                        "突破后 {} 天回踩最低 {:.2}，未破支撑位 {:.2}",
                        days_since_breakout, min_low_after_breakout, support
                    ),
                ]
            } else {
                vec![
                    format!(
                        "今日放量长阳突破{}日阻力位 {:.2}，涨幅 {:.1}%",
                        self.breakout_lookback,
                        resistance,
                        change_pct * 100.0
                    ),
                    format!("突破日成交量放大 {:.1} 倍", volume_expand),
                    format!("收盘价 {:.2} 站上均线多头排列区间", latest.close),
                ]
            };
            return Some(signal(
                self.id(),
                series,
                latest.time,
                0.8,
                &["breakout", "trend"],
                "近阶段放量长阳突破阻力位，突破后回踩未失守。",
                json!({
                    "breakout_date": current.time.format("%Y-%m-%d").to_string(),
                    "key_date": current.time.format("%Y-%m-%d").to_string(),
                    "key_date_type": "阻力位突破日",
                    "price": latest.close,
                    "resistance": resistance,
                    "support": support,
                    "breakout_change_pct": change_pct,
                    "breakout_ratio": (current.close - resistance) / resistance.max(1e-6),
                    "days_since_breakout": days_since_breakout,
                    "volume_ratio": volume_expand,
                    "ma5": ma5,
                    "ma10": ma10,
                    "ma20": ma20,
                    "reasons": reasons,
                }),
            ));
        }
        None
    }
}
