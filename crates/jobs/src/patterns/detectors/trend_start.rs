//! 趋势起点形态识别。
//!
//! 目标：
//! 识别 MACD 金叉、布林带上穿中轨、价格站上短均线等多条件同时成立的趋势启动点。
//!
//! 当前实现：
//! 1. 最新日要求 DIF 上穿 DEA，且处于 0 轴上方。
//! 2. 最新收盘价需要从布林带中轨下方向上穿越中轨。
//! 3. 最新 K 线必须收阳，并站上 5 日均线。
//! 4. 当日成交量需要高于 5 日均量一定倍数，作为启动确认。

use serde_json::json;

use super::common::{is_bullish, latest_idx, signal};
use super::PatternDetector;
use crate::patterns::indicators::SeriesIndicators;
use crate::patterns::model::{BarSeries, PatternSignal};

#[derive(Debug, Clone, Default)]
pub struct TrendStartDetector;

impl PatternDetector for TrendStartDetector {
    fn id(&self) -> &'static str {
        "trend_start"
    }

    fn detect(&self, series: &BarSeries, indicators: &SeriesIndicators) -> Option<PatternSignal> {
        let idx = latest_idx(series);
        if idx < 1 {
            return None;
        }
        let today = &series.bars[idx];
        let yesterday = &series.bars[idx - 1];
        let dif_today = indicators.dif[idx]?;
        let dif_yesterday = indicators.dif[idx - 1]?;
        let dea_today = indicators.dea[idx]?;
        let dea_yesterday = indicators.dea[idx - 1]?;
        let boll_today = indicators.boll_mid[idx]?;
        let boll_yesterday = indicators.boll_mid[idx - 1]?;
        let ma5 = indicators.ma5[idx]?;
        let volume_ma5 = indicators.volume_ma5[idx]?;

        let macd_cross = dif_today > 0.0 && dif_yesterday <= dea_yesterday && dif_today > dea_today;
        let boll_cross = yesterday.close < boll_yesterday && today.close > boll_today;
        let volume_ok = today.volume > volume_ma5 * 1.2;
        if macd_cross && boll_cross && is_bullish(today) && today.close > ma5 && volume_ok {
            return Some(signal(
                self.id(),
                series,
                today.time,
                0.79,
                &["trend", "macd", "boll"],
                "MACD金叉、布林带上穿中轨、阳线站上5日线并伴随量能放大。",
                json!({
                    "dif": dif_today,
                    "dea": dea_today,
                    "boll_mid": boll_today,
                    "ma5": ma5,
                    "volume_ratio": today.volume / volume_ma5,
                }),
            ));
        }
        None
    }
}
