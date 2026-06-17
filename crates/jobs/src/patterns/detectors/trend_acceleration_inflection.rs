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

use super::common::volume_ma;
use super::common::{linear_regression_metrics, pct_change, signal};
use super::PatternDetector;
use crate::patterns::model::{BarSeries, PatternSignal};

#[derive(Debug, Clone, Default)]
pub struct TrendAccelerationInflectionDetector;

impl PatternDetector for TrendAccelerationInflectionDetector {
    fn id(&self) -> &'static str {
        "trend_acceleration_inflection"
    }

    fn detect(
        &self,
        series: &BarSeries,
        indicators: &polars::prelude::DataFrame,
    ) -> Option<PatternSignal> {
        if series.len() < 50 {
            return None;
        }
        let closes: Vec<f64> = (series.len() - 20..series.len())
            .filter_map(|idx| series.close_at(idx))
            .collect();
        let (slope, r2) = linear_regression_metrics(&closes)?;
        if slope <= 0.0 || r2 < 0.5 {
            return None;
        }
        let latest_idx = series.len() - 1;
        for surge_idx in (series.len().saturating_sub(5)..=latest_idx).rev() {
            let change = pct_change(series, surge_idx)?;
            let vol_ma = volume_ma(indicators, surge_idx, 5)?;
            let surge_open = series.open_at(surge_idx)?;
            let surge_volume = series.volume_at(surge_idx)?;
            let surge_time = series.time_at(surge_idx)?;
            if change < 0.08 || surge_volume < vol_ma * 2.0 {
                continue;
            }
            let low_start = surge_idx.saturating_sub(40);
            let mut lowest = f64::INFINITY;
            for idx in low_start..=surge_idx {
                lowest = lowest.min(series.low_at(idx)?);
            }
            let start_price = if surge_idx > 0 {
                series.close_at(surge_idx - 1)?
            } else {
                surge_open
            };
            let distance = (start_price - lowest) / lowest.max(1e-6);
            if distance > 0.15 {
                continue;
            }
            let support_price = surge_open;
            let mut min_low = f64::INFINITY;
            for idx in surge_idx..=latest_idx {
                min_low = min_low.min(series.low_at(idx)?);
            }
            if min_low < support_price {
                continue;
            }
            let latest_time = series.time_at(latest_idx)?;
            return Some(signal(
                self.id(),
                series,
                latest_time,
                0.8,
                &["trend", "acceleration"],
                "股价处于显著上升趋势中，近端出现放量长阳加速且回调未破启动支撑。",
                json!({
                    "trend_slope": slope,
                    "trend_r2": r2,
                    "surge_date": surge_time.format("%Y-%m-%d").to_string(),
                    "distance_from_low": distance,
                    "support_price": support_price,
                    "volume_ratio": surge_volume / vol_ma,
                }),
            ));
        }
        None
    }
}
