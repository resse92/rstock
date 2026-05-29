//! 仙人指路形态识别。
//!
//! 目标：
//! 识别“上升趋势中出现长上影试盘，随后短期内被反包确认”的经典形态。
//!
//! 当前实现：
//! 1. 先要求最新日站上 5 日线，且近 20 根K线整体保持正斜率与较高拟合度。
//! 2. 回看最近 3 个交易日，寻找冲高 8% 以上、上影线占比超过 4%、放量且均线多头的信号日。
//! 3. 用信号日实体上沿到最高价的 50% 位置作为反包目标。
//! 4. 如果最新收盘价完成反包，且中间没有提前失真，则认定为仙人指路确认。

use serde_json::json;

use super::common::{latest_idx, linear_regression_metrics, signal};
use super::PatternDetector;
use crate::patterns::indicators::SeriesIndicators;
use crate::patterns::model::{BarSeries, PatternSignal};

#[derive(Debug, Clone, Default)]
pub struct ImmortalGuidanceDetector;

impl PatternDetector for ImmortalGuidanceDetector {
    fn id(&self) -> &'static str {
        "immortal_guidance"
    }

    fn detect(&self, series: &BarSeries, indicators: &SeriesIndicators) -> Option<PatternSignal> {
        let bars = &series.bars;
        if bars.len() < 30 {
            return None;
        }
        let idx = latest_idx(series);
        let today = &bars[idx];
        if today.close < indicators.ma5[idx]? || today.volume <= 0.0 {
            return None;
        }
        let closes: Vec<f64> = bars[bars.len() - 20..]
            .iter()
            .map(|bar| bar.close)
            .collect();
        let (slope, r2) = linear_regression_metrics(&closes)?;
        if slope <= 0.0 || r2 < 0.5 {
            return None;
        }
        for offset in 1..=3 {
            if idx < offset + 1 {
                break;
            }
            let signal_idx = idx - offset;
            let prev_close = bars[signal_idx - 1].close;
            let surge = (bars[signal_idx].high - prev_close) / prev_close.max(1e-6);
            let upper_shadow =
                bars[signal_idx].high - bars[signal_idx].open.max(bars[signal_idx].close);
            let shadow_ratio = upper_shadow / bars[signal_idx].high.max(1e-6);
            let volume_ma5 = indicators.volume_ma5[signal_idx]?;
            if surge < 0.08
                || shadow_ratio < 0.04
                || bars[signal_idx].volume < volume_ma5 * 1.5
                || !(indicators.ma5[signal_idx]? > indicators.ma10[signal_idx]?
                    && indicators.ma10[signal_idx]? > indicators.ma20[signal_idx]?)
            {
                continue;
            }
            let anti_body =
                (bars[signal_idx].open.max(bars[signal_idx].close) + bars[signal_idx].high) / 2.0;
            if today.close >= anti_body
                && bars[signal_idx + 1..=idx]
                    .iter()
                    .take(idx - signal_idx)
                    .all(|bar| bar.close < anti_body)
            {
                return Some(signal(
                    self.id(),
                    series,
                    today.time,
                    0.75,
                    &["upper-shadow", "trend"],
                    "冲高回落长上影后的数日内完成反包确认，符合仙人指路结构。",
                    json!({
                        "signal_day": bars[signal_idx].time.format("%Y-%m-%d").to_string(),
                        "surge_pct": surge,
                        "upper_shadow_ratio": shadow_ratio,
                        "anti_body_price": anti_body,
                        "trend_slope": slope,
                        "trend_r2": r2,
                    }),
                ));
            }
        }
        None
    }
}
