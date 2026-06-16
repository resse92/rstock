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

#[derive(Debug, Clone)]
pub struct ImmortalGuidanceDetector {
    pub surge_threshold: f64,
    pub upper_shadow_ratio: f64,
    pub volume_ratio_min: f64,
    pub trend_lookback_days: usize,
    pub trend_r_squared_threshold: f64,
    pub anti_body_ratio: f64,
    pub lookback_days: usize,
}

impl Default for ImmortalGuidanceDetector {
    fn default() -> Self {
        Self {
            surge_threshold: 0.08,
            upper_shadow_ratio: 0.04,
            volume_ratio_min: 1.5,
            trend_lookback_days: 20,
            trend_r_squared_threshold: 0.5,
            anti_body_ratio: 0.5,
            lookback_days: 3,
        }
    }
}

impl PatternDetector for ImmortalGuidanceDetector {
    fn id(&self) -> &'static str {
        "immortal_guidance"
    }

    fn detect(&self, series: &BarSeries, indicators: &SeriesIndicators) -> Option<PatternSignal> {
        if series.len() < self.trend_lookback_days.max(4) {
            return None;
        }
        let idx = latest_idx(series);
        let today = series.bar(idx)?;
        let today_ma5 = indicators.ma5[idx]?;
        if today.close < today_ma5 || today.volume <= 0.0 {
            return None;
        }
        let closes: Vec<f64> = (series.len() - self.trend_lookback_days..series.len())
            .filter_map(|idx| series.bar(idx).map(|bar| bar.close))
            .collect();
        let (slope, r2) = linear_regression_metrics(&closes)?;
        if slope <= 0.0 || r2 < self.trend_r_squared_threshold {
            return None;
        }
        for offset in 1..=self.lookback_days.min(3) {
            if idx < offset + 1 {
                break;
            }
            let signal_idx = idx - offset;
            let signal_bar = series.bar(signal_idx)?;
            let prev_close = series.bar(signal_idx - 1)?.close;
            let surge = (signal_bar.high - prev_close) / prev_close.max(1e-6);
            let upper_shadow = signal_bar.high - signal_bar.open.max(signal_bar.close);
            let shadow_ratio = upper_shadow / signal_bar.high.max(1e-6);
            let volume_ma5 = indicators.volume_ma5[signal_idx]?;
            let ma5 = indicators.ma5[signal_idx]?;
            let ma10 = indicators.ma10[signal_idx]?;
            let ma20 = indicators.ma20[signal_idx]?;
            let signal_volume_ratio = signal_bar.volume / volume_ma5.max(1e-6);
            if surge < self.surge_threshold
                || shadow_ratio < self.upper_shadow_ratio
                || signal_volume_ratio < self.volume_ratio_min
                || !(ma5 > ma10 && ma10 > ma20)
            {
                continue;
            }
            let body_top = signal_bar.open.max(signal_bar.close);
            let anti_body = body_top + (signal_bar.high - body_top) * self.anti_body_ratio;
            if today.close >= anti_body
                && (signal_idx + 1..=idx)
                    .take(idx - signal_idx)
                    .all(|idx| series.bar(idx).is_some_and(|bar| bar.close < anti_body))
            {
                let today_volume_ratio =
                    today.volume / indicators.volume_ma5[idx].unwrap_or(today.volume).max(1e-6);
                return Some(signal(
                    self.id(),
                    series,
                    today.time,
                    0.75,
                    &["upper-shadow", "trend"],
                    "冲高回落长上影后的数日内完成反包确认，符合仙人指路结构。",
                    json!({
                        "key_date": signal_bar.time.format("%Y-%m-%d").to_string(),
                        "key_date_type": "仙人指路信号日",
                        "price": today.close,
                        "volume_ratio": today_volume_ratio,
                        "signal_day": signal_bar.time.format("%Y-%m-%d").to_string(),
                        "surge_pct": surge,
                        "upper_shadow_ratio": shadow_ratio,
                        "anti_body_price": anti_body,
                        "support_level": signal_bar.open,
                        "key_day_open": signal_bar.open,
                        "key_day_close": signal_bar.close,
                        "key_day_high": signal_bar.high,
                        "ma5": ma5,
                        "ma10": ma10,
                        "ma20": ma20,
                        "signal_volume_ratio": signal_volume_ratio,
                        "days_to_confirm": offset,
                        "trend_slope": slope,
                        "trend_r2": r2,
                        "confirmation_details": {
                            "confirmed": true,
                            "confirmed_date": today.time.format("%Y-%m-%d").to_string(),
                            "days_to_confirm": offset,
                            "anti_body_price": today.close,
                            "close_above_ma5": today.close > today_ma5,
                            "post_confirmation_stable": true
                        },
                        "reasons": [
                            "仙人指路形态".to_string(),
                            format!("信号日冲高 {:.2}%，上影占比 {:.2}%", surge * 100.0, shadow_ratio * 100.0),
                            format!("信号日量比 {:.2}，均线多头排列", signal_volume_ratio),
                            format!("{} 天内收盘反包上影 50% 位置 {:.2}", offset, anti_body),
                        ],
                    }),
                ));
            }
        }
        None
    }
}
