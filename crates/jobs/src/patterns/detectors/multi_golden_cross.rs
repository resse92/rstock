//! 多金叉共振形态识别。
//!
//! 目标：
//! 识别均线金叉、KDJ 金叉、MACD 金叉在很短时间窗口内共振出现的趋势启动信号。
//!
//! 当前实现：
//! 1. 回看最近 3 个交易日，分别查找 MA5/MA20、K/D、DIF/DEA 的上穿事件。
//! 2. 三类金叉都必须出现，且最早与最晚信号之间的时间差不超过 1 个交易日。
//! 3. 同时要求最新价格仍站在短中期均线上方，且量能没有完全衰减。
//! 4. 满足条件则输出“多指标共振”信号。

use serde_json::json;

use super::common::{latest_idx, signal};
use super::PatternDetector;
use crate::patterns::indicators::SeriesIndicators;
use crate::patterns::model::{BarSeries, PatternSignal};

#[derive(Debug, Clone)]
pub struct MultiGoldenCrossDetector {
    pub lookback_days: usize,
    pub resonance_days: usize,
    pub min_volume_ratio: f64,
}

impl Default for MultiGoldenCrossDetector {
    fn default() -> Self {
        Self {
            lookback_days: 10,
            resonance_days: 3,
            min_volume_ratio: 1.0,
        }
    }
}

impl PatternDetector for MultiGoldenCrossDetector {
    fn id(&self) -> &'static str {
        "multi_golden_cross"
    }

    fn detect(&self, series: &BarSeries, indicators: &SeriesIndicators) -> Option<PatternSignal> {
        let idx = latest_idx(series);
        if idx < 3 {
            return None;
        }
        let mut ma_cross_day = None;
        let mut kdj_cross_day = None;
        let mut macd_cross_day = None;
        let start = idx.saturating_sub(self.lookback_days.saturating_sub(1));
        for day in start..=idx {
            if day == 0 {
                continue;
            }
            if indicators.ma5[day]? > indicators.ma20[day]?
                && indicators.ma5[day - 1]? <= indicators.ma20[day - 1]?
            {
                ma_cross_day = Some(day);
            }
            if indicators.k[day]? > indicators.d[day]?
                && indicators.k[day - 1]? <= indicators.d[day - 1]?
            {
                kdj_cross_day = Some(day);
            }
            if indicators.dif[day]? > indicators.dea[day]?
                && indicators.dif[day - 1]? <= indicators.dea[day - 1]?
            {
                macd_cross_day = Some(day);
            }
        }
        let (ma_day, kdj_day, macd_day) = (ma_cross_day?, kdj_cross_day?, macd_cross_day?);
        let min_day = ma_day.min(kdj_day).min(macd_day);
        let max_day = ma_day.max(kdj_day).max(macd_day);
        let latest = series.bar(idx)?;
        let ma5 = indicators.ma5[idx]?;
        let ma20 = indicators.ma20[idx]?;
        let k = indicators.k[idx]?;
        let d = indicators.d[idx]?;
        let j = indicators.j[idx]?;
        let dif = indicators.dif[idx]?;
        let dea = indicators.dea[idx]?;
        let macd = indicators.macd_hist[idx]?;
        let vol_ma5 = indicators.volume_ma5[idx]?;
        let short_trend = indicators.short_trend[idx]?;
        let bull_bear = indicators.bull_bear_line[idx]?;
        let volume_ratio = latest.volume / vol_ma5.max(1e-6);
        let max_gap = max_day - min_day;
        if max_gap <= self.resonance_days
            && latest.close > ma5
            && latest.close > ma20
            && volume_ratio >= self.min_volume_ratio
        {
            return Some(signal(
                self.id(),
                series,
                latest.time,
                0.81,
                &["golden-cross", "resonance"],
                "均线、KDJ、MACD 在短周期内形成多金叉共振。",
                json!({
                    "key_date": series.bar(min_day)?.time.format("%Y-%m-%d").to_string(),
                    "key_date_type": "多金叉共振日",
                    "ma_cross_date": series.bar(ma_day)?.time.format("%Y-%m-%d").to_string(),
                    "kdj_cross_date": series.bar(kdj_day)?.time.format("%Y-%m-%d").to_string(),
                    "macd_cross_date": series.bar(macd_day)?.time.format("%Y-%m-%d").to_string(),
                    "max_gap_days": max_gap,
                    "ma5": ma5,
                    "ma20": ma20,
                    "k": k,
                    "d": d,
                    "j": j,
                    "dif": dif,
                    "dea": dea,
                    "macd": macd,
                    "volume_ratio": volume_ratio,
                    "short_term_trend": short_trend,
                    "bull_bear_line": bull_bear,
                    "reasons": ["均线金叉", "KDJ金叉", "MACD金叉", "多指标共振"],
                }),
            ));
        }
        None
    }
}
