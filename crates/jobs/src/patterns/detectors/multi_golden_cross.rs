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

use super::common::{
    bull_bear_line, kdj_d, kdj_j, kdj_k, ma, macd_dea, macd_dif, macd_hist, short_trend, volume_ma,
};
use super::common::{latest_idx, signal};
use super::PatternDetector;
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

    fn detect(
        &self,
        series: &BarSeries,
        indicators: &polars::prelude::DataFrame,
    ) -> Option<PatternSignal> {
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
            if ma(indicators, day, 5)? > ma(indicators, day, 20)?
                && ma(indicators, day - 1, 5)? <= ma(indicators, day - 1, 20)?
            {
                ma_cross_day = Some(day);
            }
            if kdj_k(indicators, day, 9, 3, 3)? > kdj_d(indicators, day, 9, 3, 3)?
                && kdj_k(indicators, day - 1, 9, 3, 3)? <= kdj_d(indicators, day - 1, 9, 3, 3)?
            {
                kdj_cross_day = Some(day);
            }
            if macd_dif(indicators, day, 12, 26)? > macd_dea(indicators, day, 12, 26, 9)?
                && macd_dif(indicators, day - 1, 12, 26)?
                    <= macd_dea(indicators, day - 1, 12, 26, 9)?
            {
                macd_cross_day = Some(day);
            }
        }
        let (ma_day, kdj_day, macd_day) = (ma_cross_day?, kdj_cross_day?, macd_cross_day?);
        let min_day = ma_day.min(kdj_day).min(macd_day);
        let max_day = ma_day.max(kdj_day).max(macd_day);
        let latest_close = series.close_at(idx)?;
        let latest_volume = series.volume_at(idx)?;
        let latest_time = series.time_at(idx)?;
        let ma5 = ma(indicators, idx, 5)?;
        let ma20 = ma(indicators, idx, 20)?;
        let k = kdj_k(indicators, idx, 9, 3, 3)?;
        let d = kdj_d(indicators, idx, 9, 3, 3)?;
        let j = kdj_j(indicators, idx, 9, 3, 3)?;
        let dif = macd_dif(indicators, idx, 12, 26)?;
        let dea = macd_dea(indicators, idx, 12, 26, 9)?;
        let macd = macd_hist(indicators, idx, 12, 26, 9)?;
        let vol_ma5 = volume_ma(indicators, idx, 5)?;
        let short_trend = short_trend(indicators, idx, 10, 10)?;
        let bull_bear = bull_bear_line(indicators, idx, [14, 28, 57, 114])?;
        let volume_ratio = latest_volume / vol_ma5.max(1e-6);
        let max_gap = max_day - min_day;
        if max_gap <= self.resonance_days
            && latest_close > ma5
            && latest_close > ma20
            && volume_ratio >= self.min_volume_ratio
        {
            return Some(signal(
                self.id(),
                series,
                latest_time,
                0.81,
                &["golden-cross", "resonance"],
                "均线、KDJ、MACD 在短周期内形成多金叉共振。",
                json!({
                    "key_date": series.time_at(min_day)?.format("%Y-%m-%d").to_string(),
                    "key_date_type": "多金叉共振日",
                    "ma_cross_date": series.time_at(ma_day)?.format("%Y-%m-%d").to_string(),
                    "kdj_cross_date": series.time_at(kdj_day)?.format("%Y-%m-%d").to_string(),
                    "macd_cross_date": series.time_at(macd_day)?.format("%Y-%m-%d").to_string(),
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
