//! 趋势加速拐点形态识别。
//!
//! 目标：
//! 识别个股已经处于上升趋势中，并在近端出现放量长阳触发加速的结构。
//!
//! 当前实现：
//! 1. 使用最近 20 根K线的线性回归斜率和拟合度近似判断“趋势向上且较稳定”。
//! 2. 在最近 5 根K线中查找放量长阳日，要求涨幅和量能同时达标。
//! 3. 评估长阳启动位置距离最近低点的偏离，避免追到过高位置。
//! 4. 长阳之后至最新日的回调不能跌破长阳开盘价，作为趋势未破坏的确认。

use serde_json::json;

use super::common::{linear_regression_metrics, pct_change, signal};
use super::PatternDetector;
use crate::patterns::indicators::SeriesIndicators;
use crate::patterns::model::{BarSeries, PatternSignal};

#[derive(Debug, Clone, Default)]
pub struct TrendAccelerationInflectionDetector;

impl PatternDetector for TrendAccelerationInflectionDetector {
    fn id(&self) -> &'static str {
        "trend_acceleration_inflection"
    }

    fn detect(&self, series: &BarSeries, indicators: &SeriesIndicators) -> Option<PatternSignal> {
        let bars = &series.bars;
        if bars.len() < 50 {
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
        let latest_idx = bars.len() - 1;
        for surge_idx in (bars.len().saturating_sub(5)..=latest_idx).rev() {
            let change = pct_change(bars, surge_idx)?;
            let vol_ma = indicators.volume_ma5[surge_idx]?;
            if change < 0.08 || bars[surge_idx].volume < vol_ma * 2.0 {
                continue;
            }
            let low_start = surge_idx.saturating_sub(40);
            let lowest = bars[low_start..=surge_idx]
                .iter()
                .map(|bar| bar.low)
                .fold(f64::INFINITY, f64::min);
            let start_price = if surge_idx > 0 {
                bars[surge_idx - 1].close
            } else {
                bars[surge_idx].open
            };
            let distance = (start_price - lowest) / lowest.max(1e-6);
            if distance > 0.15 {
                continue;
            }
            if bars[surge_idx..=latest_idx]
                .iter()
                .map(|bar| bar.low)
                .fold(f64::INFINITY, f64::min)
                < bars[surge_idx].open
            {
                continue;
            }
            return Some(signal(
                self.id(),
                series,
                bars[latest_idx].time,
                0.8,
                &["trend", "acceleration"],
                "股价处于显著上升趋势中，近端出现放量长阳加速且回调未破启动支撑。",
                json!({
                    "trend_slope": slope,
                    "trend_r2": r2,
                    "surge_date": bars[surge_idx].time.format("%Y-%m-%d").to_string(),
                    "distance_from_low": distance,
                    "volume_ratio": bars[surge_idx].volume / vol_ma,
                }),
            ));
        }
        None
    }
}
