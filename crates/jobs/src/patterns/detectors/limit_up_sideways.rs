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

#[derive(Debug, Clone, Default)]
pub struct LimitUpSidewaysDetector;

impl PatternDetector for LimitUpSidewaysDetector {
    fn id(&self) -> &'static str {
        "limit_up_sideways"
    }

    fn detect(&self, series: &BarSeries, indicators: &SeriesIndicators) -> Option<PatternSignal> {
        let bars = &series.bars;
        if bars.len() < 15 {
            return None;
        }
        let latest_idx = latest_idx(series);

        for limit_idx in (latest_idx.saturating_sub(10)..latest_idx).rev() {
            let change = pct_change(bars, limit_idx)?;
            if change < 0.095 {
                continue;
            }
            let vol_ma = indicators.volume_ma5[limit_idx]?;
            if bars[limit_idx].volume < vol_ma * 1.8 {
                continue;
            }
            if latest_idx <= limit_idx {
                continue;
            }
            let sideways_days = latest_idx - limit_idx;
            if !(1..=10).contains(&sideways_days) {
                continue;
            }
            let highest = window_high(bars, limit_idx + 1, latest_idx);
            let lowest = window_low(bars, limit_idx + 1, latest_idx);
            let limit_close = bars[limit_idx].close;
            if highest > limit_close * 1.08 || lowest < limit_close * 0.95 {
                continue;
            }
            if bars[limit_idx + 1..=latest_idx]
                .iter()
                .all(|bar| bar.volume > bars[limit_idx].volume * 0.7)
            {
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
            let kdj_cross = prev_k <= prev_d && k > d;
            let macd_cross = prev_dif <= prev_dea && dif > dea;
            if !is_bullish(&bars[latest_idx]) || !(kdj_cross || macd_cross) {
                continue;
            }
            return Some(signal(
                self.id(),
                series,
                bars[latest_idx].time,
                0.76,
                &["limit-up", "sideways"],
                "近期涨停后横盘整理，最新一日以KDJ或MACD转强信号确认。",
                json!({
                    "limit_up_date": bars[limit_idx].time.format("%Y-%m-%d").to_string(),
                    "sideways_days": sideways_days,
                    "kdj_cross": kdj_cross,
                    "macd_cross": macd_cross,
                }),
            ));
        }
        None
    }
}
