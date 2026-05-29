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
        let first_candle = &series.bars[idx - 2];
        let second_candle = &series.bars[idx - 1];
        let third_candle = &series.bars[idx];
        let ma5 = indicators.ma5[idx]?;

        let first_body = body_ratio(first_candle);
        let second_body = body_ratio(second_candle);
        let third_change = (third_candle.close - third_candle.open) / third_candle.open.max(1e-6);
        if !is_bullish(third_candle)
            || third_change <= 0.05
            || third_candle.close <= ma5
            || !is_bearish(first_candle)
            || first_body < self.first_body_threshold
            || second_body > first_body * self.small_body_ratio
        {
            return None;
        }

        let volume_ratio = third_candle.volume / second_candle.volume.max(1e-6);
        if volume_ratio < self.volume_ratio {
            return None;
        }

        Some(signal(
            self.id(),
            series,
            third_candle.time,
            0.74,
            &["reversal", "candlestick"],
            "最近三根K线形成启明星，第三根长阳突破5日线并放量确认。",
            json!({
                "key_date": third_candle.time.format("%Y-%m-%d").to_string(),
                "first_candle_date": first_candle.time.format("%Y-%m-%d").to_string(),
                "first_candle_open": first_candle.open,
                "first_candle_close": first_candle.close,
                "second_candle_date": second_candle.time.format("%Y-%m-%d").to_string(),
                "second_candle_open": second_candle.open,
                "second_candle_close": second_candle.close,
                "third_candle_date": third_candle.time.format("%Y-%m-%d").to_string(),
                "third_candle_open": third_candle.open,
                "third_candle_close": third_candle.close,
                "first_bear_body": first_body,
                "middle_body": second_body,
                "confirm_change_pct": third_change,
                "ma5": ma5,
                "volume_ratio": volume_ratio,
                "reasons": [
                    format!("第一根长阴线实体 {:.2}%", first_body * 100.0),
                    format!("第二根小实体仅为首根实体的 {:.1}%", second_body / first_body.max(1e-6) * 100.0),
                    format!("第三根阳线涨幅 {:.2}%，并突破 MA5 {:.2}", third_change * 100.0, ma5),
                    format!("第三根成交量放大到第二根的 {:.2} 倍", volume_ratio),
                ],
            }),
        ))
    }
}
