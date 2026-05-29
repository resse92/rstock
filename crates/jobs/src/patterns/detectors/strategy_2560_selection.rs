//! 2560 战法形态识别。
//!
//! 目标：
//! 识别“价格上破 25 日均线 + 5 日均量线上穿 60 日均量线”的量价共振启动信号。
//!
//! 当前实现：
//! 1. 最新收盘价必须刚刚向上突破 25 日均线。
//! 2. 同期 5 日均量线需要上穿 60 日均量线。
//! 3. 最新收盘价还要站上 10 日均线，且单日涨幅达到最小门槛。
//! 4. 最新成交量需要至少高于 5 日均量一定比例，作为量能确认。

use serde_json::json;

use super::common::{latest_idx, signal};
use super::PatternDetector;
use crate::patterns::indicators::SeriesIndicators;
use crate::patterns::model::{BarSeries, PatternSignal};

#[derive(Debug, Clone, Default)]
pub struct Strategy2560SelectionDetector;

impl PatternDetector for Strategy2560SelectionDetector {
    fn id(&self) -> &'static str {
        "strategy_2560_selection"
    }

    fn detect(&self, series: &BarSeries, indicators: &SeriesIndicators) -> Option<PatternSignal> {
        let idx = latest_idx(series);
        if idx < 1 {
            return None;
        }
        let latest = &series.bars[idx];
        let prev = &series.bars[idx - 1];
        let ma25 = indicators.ma25[idx]?;
        let prev_ma25 = indicators.ma25[idx - 1]?;
        let vol_ma5 = indicators.volume_ma5[idx]?;
        let vol_ma60 = indicators.volume_ma60[idx]?;
        let prev_vol_ma5 = indicators.volume_ma5[idx - 1]?;
        let prev_vol_ma60 = indicators.volume_ma60[idx - 1]?;
        let ma10 = indicators.ma10[idx]?;
        let price_change = (latest.close - prev.close) / prev.close.max(1e-6);
        let price_break = latest.close > ma25 && prev.close <= prev_ma25;
        let vol_cross = vol_ma5 > vol_ma60 && prev_vol_ma5 <= prev_vol_ma60;
        if price_break
            && vol_cross
            && latest.close > ma10
            && price_change >= 0.05
            && latest.volume >= vol_ma5 * 1.2
        {
            return Some(signal(
                self.id(),
                series,
                latest.time,
                0.74,
                &["ma-break", "volume-cross"],
                "股价向上突破25日均线，同时5日均量线上穿60日均量线。",
                json!({
                    "ma25": ma25,
                    "ma10": ma10,
                    "price_change_pct": price_change,
                    "volume_ratio": latest.volume / vol_ma5,
                    "vol_ma5": vol_ma5,
                    "vol_ma60": vol_ma60,
                }),
            ));
        }
        None
    }
}
