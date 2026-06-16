//! 多方炮形态识别。
//!
//! 目标：
//! 识别“两阳夹一阴”的短线强势整理后突破结构。
//!
//! 当前实现：
//! 1. 第一根要求是中阳或大阳线，确认初始攻击力度。
//! 2. 第二根要求是缩量小阴线或弱整理线，且回撤幅度受控。
//! 3. 第三根要求重新收阳并突破第一根收盘价，最好伴随放量。
//! 4. 若三根 K 线满足上述关系，则输出多方炮确认信号。

use serde_json::json;

use super::common::{body_ratio, is_bearish, is_bullish, latest_idx, signal};
use super::PatternDetector;
use crate::patterns::indicators::SeriesIndicators;
use crate::patterns::model::{BarSeries, PatternSignal};

#[derive(Debug, Clone)]
pub struct MultiPartyCannonDetector {
    pub first_candle_rise: f64,
    pub second_candle_body_ratio: f64,
    pub second_candle_fallback_ratio: f64,
    pub third_candle_rise: f64,
    pub third_candle_breakthrough: bool,
    pub second_volume_shrink_ratio: f64,
    pub third_volume_expand_ratio: f64,
    pub third_volume_ma_ratio: f64,
    pub enable_ma_filter: bool,
    pub enable_macd_filter: bool,
    pub macd_above_zero: bool,
    pub enable_kdj_filter: bool,
    pub kdj_j_max: f64,
}

impl Default for MultiPartyCannonDetector {
    fn default() -> Self {
        Self {
            first_candle_rise: 0.03,
            second_candle_body_ratio: 0.5,
            second_candle_fallback_ratio: 0.5,
            third_candle_rise: 0.03,
            third_candle_breakthrough: true,
            second_volume_shrink_ratio: 0.8,
            third_volume_expand_ratio: 1.0,
            third_volume_ma_ratio: 1.5,
            enable_ma_filter: true,
            enable_macd_filter: false,
            macd_above_zero: true,
            enable_kdj_filter: false,
            kdj_j_max: 80.0,
        }
    }
}

impl PatternDetector for MultiPartyCannonDetector {
    fn id(&self) -> &'static str {
        "multi_party_cannon"
    }

    fn detect(&self, series: &BarSeries, indicators: &SeriesIndicators) -> Option<PatternSignal> {
        let idx = latest_idx(series);
        if idx < 2 {
            return None;
        }
        let first = series.bar(idx - 2)?;
        let second = series.bar(idx - 1)?;
        let third = series.bar(idx)?;
        let ma20 = indicators.ma20[idx];
        let vol_ma5 = indicators.volume_ma5[idx];
        let macd_hist = indicators.macd_hist[idx];
        let j = indicators.j[idx];

        let first_rise = (first.close - first.open) / first.open.max(1e-6);
        let third_rise = (third.close - third.open) / third.open.max(1e-6);
        let first_body = body_ratio(&first);
        let second_body = body_ratio(&second);
        let first_body_abs = (first.close - first.open).abs();
        let fallback = (first.close - second.close).max(0.0) / first_body_abs.max(1e-6);

        if !is_bullish(&first)
            || first_rise < self.first_candle_rise
            || !is_bearish(&second)
            || second_body > first_body * self.second_candle_body_ratio
            || fallback > self.second_candle_fallback_ratio
            || !is_bullish(&third)
            || third_rise < self.third_candle_rise
        {
            return None;
        }

        if self.third_candle_breakthrough && third.close <= first.close {
            return None;
        }
        if second.volume > first.volume * self.second_volume_shrink_ratio {
            return None;
        }
        if third.volume <= first.volume * self.third_volume_expand_ratio {
            return None;
        }
        if vol_ma5
            .map(|value| third.volume < value * self.third_volume_ma_ratio)
            .unwrap_or(false)
        {
            return None;
        }
        if self.enable_ma_filter && ma20.map(|value| third.close < value).unwrap_or(true) {
            return None;
        }
        if self.enable_macd_filter
            && macd_hist
                .map(|value| self.macd_above_zero && value <= 0.0)
                .unwrap_or(true)
        {
            return None;
        }
        if self.enable_kdj_filter && j.map(|value| value >= self.kdj_j_max).unwrap_or(true) {
            return None;
        }

        let category = if first_rise >= 0.07 && third_rise >= 0.07 {
            "strong"
        } else if (0.03..0.07).contains(&first_rise) && (0.03..0.07).contains(&third_rise) {
            "standard"
        } else if (0.01..0.03).contains(&first_rise) && (0.01..0.03).contains(&third_rise) {
            "weak"
        } else {
            "standard"
        };
        let third_volume_ratio = third.volume / first.volume.max(1e-6);
        let third_vs_ma5_volume = vol_ma5.map(|value| third.volume / value).unwrap_or(0.0);
        let fallback_pct = fallback * 100.0;
        let reasons = vec![
            format!("第一根阳线涨幅 {:.2}%", first_rise * 100.0),
            format!(
                "第二根阴线回调 {:.2}%，实体仅为首根的 {:.1}%",
                fallback_pct,
                second_body / first_body.max(1e-6) * 100.0
            ),
            format!(
                "第三根阳线涨幅 {:.2}%，{}第一根收盘价",
                third_rise * 100.0,
                if third.close > first.close {
                    "突破"
                } else {
                    "逼近"
                }
            ),
            format!(
                "第三根成交量是第一根的 {:.2} 倍，较 5 日均量 {:.2} 倍",
                third_volume_ratio, third_vs_ma5_volume
            ),
        ];

        Some(signal(
            self.id(),
            series,
            third.time,
            0.73,
            &["candlestick", "breakout"],
            "最近三根K线构成两阳夹一阴的多方炮，第三根放量突破前高。",
            json!({
                "key_date": third.time.format("%Y-%m-%d").to_string(),
                "pattern_category": category,
                "first_rise_pct": first_rise,
                "second_body_ratio": second_body / first_body.max(1e-6),
                "fallback_ratio": fallback,
                "third_rise_pct": third_rise,
                "third_volume_ratio": third_volume_ratio,
                "third_vs_ma5_volume": third_vs_ma5_volume,
                "ma20": ma20,
                "macd_hist": macd_hist,
                "j": j,
                "reasons": reasons,
            }),
        ))
    }
}
