//! 强势洗盘弱转强形态识别。
//!
//! 目标：
//! 识别个股先出现放量大阳线，随后通过洗盘阴线清洗浮筹，再以反包阳线恢复强势的结构。
//!
//! 当前实现：
//! 1. 在最近若干交易日内寻找放量大阳线。
//! 2. 次日要求是明显阴线，且成交量不低，作为洗盘特征。
//! 3. 随后 3 个交易日内寻找反包阳线，收盘价重新站上关键价位。
//! 4. 反包后到最新日之间不能再次明显转弱，否则不判定为成功弱转强。

use serde_json::json;

use super::common::{is_bearish, is_bullish, pct_change, signal};
use super::PatternDetector;
use crate::patterns::indicators::SeriesIndicators;
use crate::patterns::model::{BarSeries, PatternSignal};

#[derive(Debug, Clone, Default)]
pub struct StrongWashWeakToStrongDetector;

impl PatternDetector for StrongWashWeakToStrongDetector {
    fn id(&self) -> &'static str {
        "strong_wash_weak_to_strong"
    }

    fn detect(&self, series: &BarSeries, indicators: &SeriesIndicators) -> Option<PatternSignal> {
        let bars = &series.bars;
        if bars.len() < 10 {
            return None;
        }
        let end = bars.len();
        let latest_idx = end - 1;
        for big_idx in (end.saturating_sub(6)..end.saturating_sub(2)).rev() {
            let big_change = pct_change(bars, big_idx)?;
            let big_vol_ma = indicators.volume_ma5[big_idx]?;
            if big_change < 0.08
                || !is_bullish(&bars[big_idx])
                || bars[big_idx].volume < big_vol_ma * 1.5
            {
                continue;
            }
            let wash_idx = big_idx + 1;
            if wash_idx >= end
                || !is_bearish(&bars[wash_idx])
                || bars[wash_idx].volume < bars[big_idx].volume * 1.2
            {
                continue;
            }
            let reversal_end = (wash_idx + 3).min(end - 1);
            for reversal_idx in wash_idx + 1..=reversal_end {
                if !is_bullish(&bars[reversal_idx]) {
                    continue;
                }
                if bars[reversal_idx].close <= bars[big_idx].close
                    && bars[reversal_idx].close <= bars[wash_idx].open
                {
                    continue;
                }
                if latest_idx.saturating_sub(reversal_idx) > 5 {
                    continue;
                }
                if reversal_idx < latest_idx
                    && bars[reversal_idx + 1..end]
                        .iter()
                        .any(|bar| bar.close <= bars[big_idx].close)
                {
                    continue;
                }
                if bars[reversal_idx..end]
                    .iter()
                    .any(|bar| bar.close <= bars[big_idx].close)
                {
                    continue;
                }
                return Some(signal(
                    self.id(),
                    series,
                    bars[end - 1].time,
                    0.77,
                    &["wash", "reversal"],
                    "先出现放量大阳线，随后洗盘阴线，再以反包阳线完成弱转强。",
                    json!({
                        "big_candle_date": bars[big_idx].time.format("%Y-%m-%d").to_string(),
                        "wash_date": bars[wash_idx].time.format("%Y-%m-%d").to_string(),
                        "reversal_date": bars[reversal_idx].time.format("%Y-%m-%d").to_string(),
                        "big_candle_volume_ratio": bars[big_idx].volume / big_vol_ma.max(1e-6),
                        "wash_volume_ratio": bars[wash_idx].volume / bars[big_idx].volume.max(1e-6),
                        "days_after_reversal": latest_idx - reversal_idx,
                    }),
                ));
            }
        }
        None
    }
}
