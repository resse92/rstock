//! W 底形态识别。
//!
//! 目标：
//! 识别双底结构完成后，股价放量突破颈线的反转信号。
//!
//! 当前实现：
//! 1. 在最近 40 根K线中扫描局部低点，提取左右两个底部候选。
//! 2. 两个底部之间必须有足够时间间隔，且低点价差不能过大。
//! 3. 取两个低点之间的最高价近似为颈线，并要求颈线有足够高度。
//! 4. 最新日如果以放量方式突破颈线，且短中期均线转强，则认定为 W 底完成。

use serde_json::json;

use super::common::{pct_change, signal, window_high};
use super::PatternDetector;
use crate::patterns::indicators::SeriesIndicators;
use crate::patterns::model::{BarSeries, PatternSignal};

#[derive(Debug, Clone)]
pub struct WBottomDetector {
    pub pattern_days: usize,
    pub min_gap: usize,
    pub bottom_diff_threshold: f64,
    pub breakout_ratio: f64,
    pub volume_expand_ratio: f64,
    pub support_ratio: f64,
    pub support_days: usize,
    pub volume_shrink_ratio: f64,
}

impl Default for WBottomDetector {
    fn default() -> Self {
        Self {
            pattern_days: 40,
            min_gap: 10,
            bottom_diff_threshold: 0.03,
            breakout_ratio: 0.01,
            volume_expand_ratio: 1.2,
            support_ratio: 0.98,
            support_days: 20,
            volume_shrink_ratio: 0.8,
        }
    }
}

impl PatternDetector for WBottomDetector {
    fn id(&self) -> &'static str {
        "w_bottom"
    }

    fn detect(&self, series: &BarSeries, indicators: &SeriesIndicators) -> Option<PatternSignal> {
        let bars = &series.bars;
        if bars.len() < self.pattern_days {
            return None;
        }
        let end = bars.len();
        let latest_idx = end - 1;
        let scan_start = end.saturating_sub(self.pattern_days);
        let mut lows = Vec::new();
        for idx in scan_start + 2..latest_idx.saturating_sub(5) {
            let low = bars[idx].low;
            if low <= bars[idx - 1].low
                && low <= bars[idx - 2].low
                && low <= bars[idx + 1].low
                && low <= bars[idx + 2].low
            {
                lows.push(idx);
            }
        }
        if lows.len() < 2 {
            return None;
        }
        let l1 = lows[lows.len() - 2];
        let l2 = lows[lows.len() - 1];
        if l2 <= l1 + self.min_gap {
            return None;
        }
        let diff = (bars[l1].low - bars[l2].low).abs() / bars[l1].low.max(1e-6);
        if diff > self.bottom_diff_threshold {
            return None;
        }
        let neckline = window_high(bars, l1, l2);
        if neckline < bars[l1].low * 1.1 {
            return None;
        }

        let mut break_idx = None;
        let break_search_start = end.saturating_sub(5);
        for idx in break_search_start..end {
            let change = pct_change(bars, idx)?;
            let vol_ma = indicators.volume_ma5[idx]?;
            if change > 0.08
                && bars[idx].volume >= vol_ma * self.volume_expand_ratio
                && bars[idx].close >= neckline * (1.0 + self.breakout_ratio)
                && idx > 0
                && bars[idx - 1].close < neckline
            {
                break_idx = Some(idx);
                break;
            }
        }
        let break_idx = break_idx?;

        let trend_ok = indicators.ma10[latest_idx]? > indicators.ma30[latest_idx]?;
        if !trend_ok {
            return None;
        }

        let before_start = l1.saturating_add(1);
        let before_end = (l1 + 31).min(end);
        if before_start >= before_end {
            return None;
        }
        let max_high_before_l1 = bars[before_start..before_end]
            .iter()
            .map(|bar| bar.high)
            .fold(f64::NEG_INFINITY, f64::max);
        if max_high_before_l1 <= bars[l1].low * 1.2 {
            return None;
        }

        let support_price = neckline * self.support_ratio;
        let support_end = (break_idx + self.support_days).min(latest_idx);
        if break_idx < support_end
            && bars[break_idx + 1..=support_end]
                .iter()
                .any(|bar| bar.close < support_price)
        {
            return None;
        }

        let l1_volume = bars[l1].volume;
        let l2_volume = bars[l2].volume;
        let volume_shrink_ratio = if l1_volume > 0.0 {
            l2_volume / l1_volume
        } else {
            0.0
        };
        let volume_shrink = l1_volume > 0.0 && l2_volume < l1_volume * self.volume_shrink_ratio;
        if break_idx >= latest_idx.saturating_sub(20) {
            return Some(signal(
                self.id(),
                series,
                bars[latest_idx].time,
                0.79,
                &["double-bottom", "breakout"],
                "双底结构完成后在近端放量突破颈线。",
                json!({
                    "key_date": bars[break_idx].time.format("%Y-%m-%d").to_string(),
                    "key_date_type": "颈线突破日",
                    "left_bottom_date": bars[l1].time.format("%Y-%m-%d").to_string(),
                    "right_bottom_date": bars[l2].time.format("%Y-%m-%d").to_string(),
                    "break_date": bars[break_idx].time.format("%Y-%m-%d").to_string(),
                    "neckline": neckline,
                    "bottom_diff_ratio": diff,
                    "break_volume_ratio": bars[break_idx].volume / indicators.volume_ma5[break_idx]?.max(1e-6),
                    "support_price": support_price,
                    "trend_reversal": trend_ok,
                    "volume_shrink": volume_shrink,
                    "volume_shrink_ratio": volume_shrink_ratio,
                    "reasons": [
                        format!("双底价差 {:.2}%，间隔 {} 天", diff * 100.0, l2 - l1),
                        format!("颈线 {:.2}，突破日放量 {:.2} 倍", neckline, bars[break_idx].volume / indicators.volume_ma5[break_idx]?.max(1e-6)),
                        format!("突破后支撑位 {:.2} 未被跌破", support_price),
                    ],
                }),
            ));
        }
        None
    }
}
