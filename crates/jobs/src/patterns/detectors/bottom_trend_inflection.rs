//! 底部趋势拐点形态识别。
//!
//! 目标：
//! 识别经历中期深跌后，在底部区域出现放量反弹并伴随动能改善的反转信号。
//!
//! 当前实现：
//! 1. 回看 120 根K线，要求从阶段高点到后续低点的跌幅达到 45%。
//! 2. 最新交易日涨幅达到 8%，且成交量至少是 10 日均量的 2.5 倍。
//! 3. 最近 20 根K线里，价格低点线性回归斜率仍偏弱，但 MACD 柱动能斜率相对改善，
//!    作为“底背离/跌势衰减”的近似判定。
//! 4. 满足上述条件后输出底部反转信号。

use serde_json::json;

use super::common::{linear_regression_metrics, pct_change, signal};
use super::PatternDetector;
use crate::patterns::indicators::SeriesIndicators;
use crate::patterns::model::{BarSeries, PatternSignal};

#[derive(Debug, Clone, Default)]
pub struct BottomTrendInflectionDetector;

impl PatternDetector for BottomTrendInflectionDetector {
    fn id(&self) -> &'static str {
        "bottom_trend_inflection"
    }

    fn detect(&self, series: &BarSeries, indicators: &SeriesIndicators) -> Option<PatternSignal> {
        // 取出K线序列，后续所有条件都基于它做计算。
        let bars = &series.bars;
        // 至少需要120根K线，才能完成“半年深跌 + 底部拐点”的判断。
        if bars.len() < 120 {
            return None;
        }
        // `end` 指向序列末尾，`start` 是120根回看窗口的起点。
        let end = bars.len();
        let start = end - 120;
        // 找出120根窗口里的最高价，作为中期跌幅的起点参考。
        let highest = bars[start..end]
            .iter()
            .map(|bar| bar.high)
            .fold(f64::NEG_INFINITY, f64::max);
        // 同时找出该最高价所在位置，后面只统计“高点之后”的下跌幅度。
        let highest_idx = bars[start..end]
            .iter()
            .enumerate()
            .max_by(|a, b| a.1.high.partial_cmp(&b.1.high).unwrap())
            .map(|(idx, _)| idx + start)?;
        // 从阶段高点往后看，找出后续最低点，衡量是否经历过明显深跌。
        let lowest_after_high = bars[highest_idx..end]
            .iter()
            .map(|bar| bar.low)
            .fold(f64::INFINITY, f64::min);
        // 计算从阶段高点到后续最低点的跌幅比例。
        let decline = (highest - lowest_after_high) / highest.max(1e-6);
        // 跌幅不足45%，说明还不够“深跌到底部”，直接忽略。
        if decline < 0.45 {
            return None;
        }
        // 最近20根K线用于估算价格低点和MACD柱的趋势变化。
        let recent_slice = end.saturating_sub(20);
        // 提取最近20根的最低价序列，观察价格是否仍在缓慢下探。
        let recent_lows: Vec<f64> = bars[recent_slice..end].iter().map(|bar| bar.low).collect();
        // 提取最近20根的MACD柱序列；缺失值按0处理，避免中断检测。
        let recent_macd: Vec<f64> = indicators.macd_hist[recent_slice..end]
            .iter()
            .map(|value| value.unwrap_or(0.0))
            .collect();
        // 对价格低点做线性回归，得到价格趋势斜率与拟合度。
        let price_reg = linear_regression_metrics(&recent_lows)?;
        // 对MACD柱做线性回归，得到动能趋势斜率与拟合度。
        let macd_reg = linear_regression_metrics(&recent_macd)?;
        // 要求价格低点斜率仍为负，但MACD斜率比价格更强，近似表示底背离/跌势衰减。
        if !(price_reg.0 < 0.0 && macd_reg.0 > price_reg.0) {
            return None;
        }

        // 只在最近10根内寻找那根“放量大阳”式的启动K线。
        let surge_window_start = end.saturating_sub(10);
        // 从近到远倒序扫描，优先使用最新的启动日。
        for surge_idx in (surge_window_start..end).rev() {
            // 计算该日相对前一日的涨幅。
            let day_change = pct_change(bars, surge_idx)?;
            // 读取该日对应的10日均量，衡量是否放量。
            let vol_ma10 = indicators.volume_ma10[surge_idx]?;
            // 涨幅不超过8%或成交量不到10日均量的2.5倍，都不算有效启动。
            if day_change <= 0.08 || bars[surge_idx].volume < vol_ma10 * 2.5 {
                continue;
            }

            // 再找出整个120根窗口内的绝对低点，用来判断启动位置是否仍在底部附近。
            let lowest_price = bars[start..end]
                .iter()
                .map(|bar| bar.low)
                .fold(f64::INFINITY, f64::min);
            // 计算启动日收盘价距离阶段最低点的偏离比例。
            let distance_ratio = (bars[surge_idx].close - lowest_price) / lowest_price.max(1e-6);
            // 如果启动时离最低点已经超过15%，说明更像追高而非底部反转。
            if distance_ratio > 0.15 {
                continue;
            }

            // 把启动日开盘价视为一个短线支撑位。
            let support_price = bars[surge_idx].open;
            // 启动日之后的区间从下一根K线开始检查。
            let after_surge_start = surge_idx.saturating_add(1);
            // 如果后续又跌破启动日开盘价，说明支撑不稳，反转信号失效。
            if after_surge_start < end
                && bars[after_surge_start..end]
                    .iter()
                    .any(|bar| bar.low < support_price)
            {
                continue;
            }

            // 所有条件满足后，构造并返回一个底部趋势拐点信号。
            return Some(signal(
                // 信号ID，对应当前检测器名称。
                self.id(),
                // 原始序列信息，便于写入标的与交易所等上下文。
                series,
                // 信号日期使用最新一根K线日期，而不是启动日日期。
                bars[end - 1].time,
                // 当前规则给一个固定置信分数。
                0.78,
                // 给信号打上“底部”“反转”标签。
                &["bottom", "reversal"],
                // 面向外部展示的人类可读解释。
                "半年深跌后出现放量反弹，价格创新低力度减弱且MACD出现底背离迹象。",
                // 附带关键证据，便于后续排查与展示。
                json!({
                    "decline_ratio": decline,
                    "surge_date": bars[surge_idx].time.format("%Y-%m-%d").to_string(),
                    "surge_change_pct": day_change,
                    "volume_ratio": bars[surge_idx].volume / vol_ma10,
                    "distance_from_low": distance_ratio,
                    "support_price": support_price,
                    "price_slope": price_reg.0,
                    "macd_slope": macd_reg.0,
                }),
            ));
        }

        // 最近10根内没有找到满足全部条件的启动日，则不输出信号。
        None
    }
}
