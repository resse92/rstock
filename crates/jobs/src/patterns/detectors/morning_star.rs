//! 启明星形态识别。
//!
//! 目标：
//! 识别三根 K 线构成的底部反转结构：长阴线、小实体犹豫线、长阳确认线。
//!
//! 当前实现：
//! 1. 倒数第三根要求是明显长阴线，表示前一段下跌压力。
//! 2. 倒数第二根要求实体很小，体现市场犹豫。
//! 3. 最新一根要求为放量长阳，并站上 5 日线。
//! 4. 若三者顺序和强弱关系成立，则认定为启明星完成确认。

use serde_json::json;

use super::common::{body_ratio, is_bearish, is_bullish, latest_idx, signal};
use super::PatternDetector;
use crate::patterns::indicators::SeriesIndicators;
use crate::patterns::model::{BarSeries, PatternSignal};

#[derive(Debug, Clone)]
pub struct MorningStarDetector {
    pub first_body_threshold: f64,
    pub small_body_ratio: f64,
    pub volume_ratio: f64,
}

impl Default for MorningStarDetector {
    fn default() -> Self {
        Self {
            first_body_threshold: 0.03,
            small_body_ratio: 0.3,
            volume_ratio: 1.5,
        }
    }
}

impl PatternDetector for MorningStarDetector {
    fn id(&self) -> &'static str {
        "morning_star"
    }

    fn detect(&self, series: &BarSeries, indicators: &SeriesIndicators) -> Option<PatternSignal> {
        let idx = latest_idx(series);
        if idx < 2 {
            return None;
        }
        let third = &series.bars[idx - 2];
        let second = &series.bars[idx - 1];
        let first = &series.bars[idx];
        let ma5 = indicators.ma5[idx]?;

        let third_body = body_ratio(third);
        let second_body = body_ratio(second);
        let first_change = (first.close - first.open) / first.open.max(1e-6);
        if is_bearish(third)
            && third_body >= self.first_body_threshold
            && second_body <= third_body * self.small_body_ratio
            && is_bullish(first)
            && first_change > 0.05
            && first.close > ma5
            && first.volume >= second.volume * self.volume_ratio
        {
            return Some(signal(
                self.id(),
                series,
                first.time,
                0.74,
                &["reversal", "candlestick"],
                "最近三根K线形成启明星，第三根长阳突破5日线并放量确认。",
                json!({
                    "first_bear_body": third_body,
                    "middle_body": second_body,
                    "confirm_change_pct": first_change,
                    "volume_ratio": first.volume / second.volume.max(1e-6),
                }),
            ));
        }
        None
    }
}
