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

#[derive(Debug, Clone, Default)]
pub struct MultiPartyCannonDetector;

impl PatternDetector for MultiPartyCannonDetector {
    fn id(&self) -> &'static str {
        "multi_party_cannon"
    }

    fn detect(&self, series: &BarSeries, indicators: &SeriesIndicators) -> Option<PatternSignal> {
        let idx = latest_idx(series);
        if idx < 2 {
            return None;
        }
        let first = &series.bars[idx - 2];
        let second = &series.bars[idx - 1];
        let third = &series.bars[idx];
        let ma20 = indicators.ma20[idx];

        let first_rise = (first.close - first.open) / first.open.max(1e-6);
        let third_rise = (third.close - third.open) / third.open.max(1e-6);
        let first_body = body_ratio(first);
        let second_body = body_ratio(second);
        let first_body_abs = (first.close - first.open).abs();
        let fallback = (first.close - second.close).max(0.0) / first_body_abs.max(1e-6);
        if is_bullish(first)
            && first_rise >= 0.03
            && is_bearish(second)
            && second_body <= first_body * 0.5
            && fallback <= 0.5
            && second.volume <= first.volume * 0.8
            && is_bullish(third)
            && third_rise >= 0.03
            && third.close > first.close
            && third.volume >= first.volume
            && ma20.map(|value| third.close > value).unwrap_or(true)
        {
            return Some(signal(
                self.id(),
                series,
                third.time,
                0.73,
                &["candlestick", "breakout"],
                "最近三根K线构成两阳夹一阴的多方炮，第三根放量突破前高。",
                json!({
                    "first_rise_pct": first_rise,
                    "second_body_ratio": second_body / first_body.max(1e-6),
                    "fallback_ratio": fallback,
                    "third_rise_pct": third_rise,
                    "third_volume_ratio": third.volume / first.volume.max(1e-6),
                }),
            ));
        }
        None
    }
}
