//! 趋势共振反转形态识别。
//!
//! 目标：
//! 识别 RSI 突破、短中期均线金叉、MACD 金叉在短时间内共振出现的底部反转信号。
//!
//! 当前实现：
//! 1. 先在最近若干天内查找 RSI(14) 从低位超卖区突破到 50 上方的信号日。
//! 2. 再在相邻窗口中查找 MA5/MA20 金叉与 DIF/DEA 金叉。
//! 3. 三个信号必须全部存在，且最早与最晚之间的时间差不超过设定共振窗口。
//! 4. 命中后以 RSI 突破日作为关键日期输出。

use serde_json::json;

use super::common::{latest_idx, signal};
use super::PatternDetector;
use crate::patterns::indicators::SeriesIndicators;
use crate::patterns::model::{BarSeries, PatternSignal};

#[derive(Debug, Clone)]
pub struct TrendResonanceReversalDetector {
    pub rsi_oversold: f64,
    pub rsi_breakout: f64,
    pub signal_days: usize,
    pub lookback_days: usize,
}

impl Default for TrendResonanceReversalDetector {
    fn default() -> Self {
        Self {
            rsi_oversold: 30.0,
            rsi_breakout: 50.0,
            signal_days: 3,
            lookback_days: 5,
        }
    }
}

impl PatternDetector for TrendResonanceReversalDetector {
    fn id(&self) -> &'static str {
        "trend_resonance_reversal"
    }

    fn detect(&self, series: &BarSeries, indicators: &SeriesIndicators) -> Option<PatternSignal> {
        let idx = latest_idx(series);
        if idx < 30 {
            return None;
        }

        let rsi_search_start = idx.saturating_sub(self.signal_days.saturating_sub(1));
        let mut rsi_breakout_day = None;
        for day in rsi_search_start..=idx {
            if day == 0 {
                continue;
            }
            let rsi_now = indicators.rsi14[day]?;
            let rsi_prev = indicators.rsi14[day - 1]?;
            if rsi_now < self.rsi_breakout || rsi_prev >= self.rsi_breakout {
                continue;
            }

            let lookback_start = day.saturating_sub(self.lookback_days);
            let mut min_rsi = f64::INFINITY;
            for probe in lookback_start..day {
                if let Some(value) = indicators.rsi14[probe] {
                    min_rsi = min_rsi.min(value);
                }
            }
            if min_rsi <= self.rsi_oversold {
                rsi_breakout_day = Some(day);
                break;
            }
        }
        let rsi_day = rsi_breakout_day?;

        let search_start = idx.saturating_sub(self.signal_days * 2);
        let mut ma_cross_day = None;
        let mut macd_cross_day = None;
        for day in search_start..=idx {
            if day == 0 {
                continue;
            }
            if ma_cross_day.is_none()
                && indicators.ma5[day]? > indicators.ma20[day]?
                && indicators.ma5[day - 1]? <= indicators.ma20[day - 1]?
            {
                ma_cross_day = Some(day);
            }
            if macd_cross_day.is_none()
                && indicators.dif[day]? > indicators.dea[day]?
                && indicators.dif[day - 1]? <= indicators.dea[day - 1]?
            {
                macd_cross_day = Some(day);
            }
            if ma_cross_day.is_some() && macd_cross_day.is_some() {
                break;
            }
        }

        let ma_day = ma_cross_day?;
        let macd_day = macd_cross_day?;
        let min_day = rsi_day.min(ma_day).min(macd_day);
        let max_day = rsi_day.max(ma_day).max(macd_day);
        if max_day - min_day > self.signal_days {
            return None;
        }

        Some(signal(
            self.id(),
            series,
            series.bar(rsi_day)?.time,
            0.8,
            &["resonance", "reversal", "rsi"],
            "RSI突破、均线金叉与MACD金叉在短窗口内共振，符合趋势共振反转结构。",
            json!({
                "key_date": series.bar(rsi_day)?.time.format("%Y-%m-%d").to_string(),
                "key_date_type": "RSI突破日",
                "rsi_breakout_day": series.bar(rsi_day)?.time.format("%Y-%m-%d").to_string(),
                "ma_cross_day": series.bar(ma_day)?.time.format("%Y-%m-%d").to_string(),
                "macd_cross_day": series.bar(macd_day)?.time.format("%Y-%m-%d").to_string(),
                "rsi_value": indicators.rsi14[rsi_day],
                "ma_short": indicators.ma5[ma_day],
                "ma_long": indicators.ma20[ma_day],
                "macd_dif": indicators.dif[macd_day],
                "macd_dea": indicators.dea[macd_day],
                "close": series.bar(rsi_day)?.close,
                "max_time_diff": max_day - min_day,
                "reasons": [
                    format!("RSI 从超卖区突破到 {:.0} 上方", self.rsi_breakout),
                    "MA5 上穿 MA20".to_string(),
                    "MACD 金叉".to_string(),
                ],
            }),
        ))
    }
}
