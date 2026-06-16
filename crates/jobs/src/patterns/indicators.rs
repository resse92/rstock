use crate::patterns::model::BarSeries;

#[derive(Debug, Clone)]
pub struct SeriesIndicators {
    pub ma5: Vec<Option<f64>>,
    pub ma10: Vec<Option<f64>>,
    pub ma20: Vec<Option<f64>>,
    pub ma25: Vec<Option<f64>>,
    pub ma30: Vec<Option<f64>>,
    pub volume_ma5: Vec<Option<f64>>,
    pub volume_ma10: Vec<Option<f64>>,
    pub volume_ma60: Vec<Option<f64>>,
    pub dif: Vec<Option<f64>>,
    pub dea: Vec<Option<f64>>,
    pub macd_hist: Vec<Option<f64>>,
    pub k: Vec<Option<f64>>,
    pub d: Vec<Option<f64>>,
    pub j: Vec<Option<f64>>,
    pub boll_mid: Vec<Option<f64>>,
    pub boll_upper: Vec<Option<f64>>,
    pub boll_lower: Vec<Option<f64>>,
    pub rsi14: Vec<Option<f64>>,
    pub short_trend: Vec<Option<f64>>,
    pub bull_bear_line: Vec<Option<f64>>,
}

impl SeriesIndicators {
    pub fn calculate(series: &BarSeries) -> Self {
        let closes = f64_values(series, "close");
        let highs = f64_values(series, "high");
        let lows = f64_values(series, "low");
        let volumes = f64_values(series, "volume");

        let ma5 = rolling_mean(&closes, 5);
        let ma10 = rolling_mean(&closes, 10);
        let ma20 = rolling_mean(&closes, 20);
        let ma25 = rolling_mean(&closes, 25);
        let ma30 = rolling_mean(&closes, 30);
        let volume_ma5 = rolling_mean(&volumes, 5);
        let volume_ma10 = rolling_mean(&volumes, 10);
        let volume_ma60 = rolling_mean(&volumes, 60);

        let ema12 = ema(&closes, 12);
        let ema26 = ema(&closes, 26);
        let dif: Vec<Option<f64>> = ema12
            .iter()
            .zip(ema26.iter())
            .map(|(fast, slow)| match (fast, slow) {
                (Some(f), Some(s)) => Some(f - s),
                _ => None,
            })
            .collect();
        let dea = ema_optional(&dif, 9);
        let macd_hist = dif
            .iter()
            .zip(dea.iter())
            .map(|(d, e)| match (d, e) {
                (Some(dif_value), Some(dea_value)) => Some(dif_value - dea_value),
                _ => None,
            })
            .collect();

        let (k, d, j) = kdj(&highs, &lows, &closes, 9, 3, 3);
        let boll_mid = rolling_mean(&closes, 20);
        let boll_std = rolling_std(&closes, 20);
        let (boll_upper, boll_lower): (Vec<_>, Vec<_>) = boll_mid
            .iter()
            .zip(boll_std.iter())
            .map(|(mid, std)| match (mid, std) {
                (Some(mid_value), Some(std_value)) => (
                    Some(mid_value + 2.0 * std_value),
                    Some(mid_value - 2.0 * std_value),
                ),
                _ => (None, None),
            })
            .unzip();
        let rsi14 = rsi(&closes, 14);

        let short_trend = ema_optional(&ema(&closes, 10), 10);
        let ma14 = rolling_mean(&closes, 14);
        let ma28 = rolling_mean(&closes, 28);
        let ma57 = rolling_mean(&closes, 57);
        let ma114 = rolling_mean(&closes, 114);
        let bull_bear_line = (0..closes.len())
            .map(|idx| match (ma14[idx], ma28[idx], ma57[idx], ma114[idx]) {
                (Some(a), Some(b), Some(c), Some(dv)) => Some((a + b + c + dv) / 4.0),
                _ => None,
            })
            .collect();

        Self {
            ma5,
            ma10,
            ma20,
            ma25,
            ma30,
            volume_ma5,
            volume_ma10,
            volume_ma60,
            dif,
            dea,
            macd_hist,
            k,
            d,
            j,
            boll_mid,
            boll_upper,
            boll_lower,
            rsi14,
            short_trend,
            bull_bear_line,
        }
    }
}

fn f64_values(series: &BarSeries, column: &str) -> Vec<f64> {
    series
        .frame
        .column(column)
        .ok()
        .and_then(|col| col.f64().ok())
        .map(|values| {
            (0..series.frame.height())
                .map(|idx| values.get(idx).unwrap_or_default())
                .collect()
        })
        .unwrap_or_else(|| match column {
            "close" => (0..series.len())
                .filter_map(|idx| series.bar(idx).map(|bar| bar.close))
                .collect(),
            "high" => (0..series.len())
                .filter_map(|idx| series.bar(idx).map(|bar| bar.high))
                .collect(),
            "low" => (0..series.len())
                .filter_map(|idx| series.bar(idx).map(|bar| bar.low))
                .collect(),
            "volume" => (0..series.len())
                .filter_map(|idx| series.bar(idx).map(|bar| bar.volume))
                .collect(),
            _ => Vec::new(),
        })
}

fn rolling_mean(values: &[f64], window: usize) -> Vec<Option<f64>> {
    let mut out = Vec::with_capacity(values.len());
    let mut sum = 0.0;
    for (idx, value) in values.iter().enumerate() {
        sum += value;
        if idx >= window {
            sum -= values[idx - window];
        }
        if idx + 1 >= window {
            out.push(Some(sum / window as f64));
        } else {
            out.push(None);
        }
    }
    out
}

fn rolling_std(values: &[f64], window: usize) -> Vec<Option<f64>> {
    let mean = rolling_mean(values, window);
    let mut out = Vec::with_capacity(values.len());
    for idx in 0..values.len() {
        if idx + 1 < window {
            out.push(None);
            continue;
        }
        let start = idx + 1 - window;
        let slice = &values[start..=idx];
        let avg = mean[idx].unwrap_or_default();
        let variance = slice.iter().map(|value| (value - avg).powi(2)).sum::<f64>() / window as f64;
        out.push(Some(variance.sqrt()));
    }
    out
}

fn ema(values: &[f64], span: usize) -> Vec<Option<f64>> {
    let mut out = Vec::with_capacity(values.len());
    if values.is_empty() {
        return out;
    }
    let alpha = 2.0 / (span as f64 + 1.0);
    let mut prev = values[0];
    for (idx, value) in values.iter().enumerate() {
        if idx == 0 {
            prev = *value;
        } else {
            prev = alpha * value + (1.0 - alpha) * prev;
        }
        out.push(Some(prev));
    }
    out
}

fn ema_optional(values: &[Option<f64>], span: usize) -> Vec<Option<f64>> {
    let mut out = Vec::with_capacity(values.len());
    let alpha = 2.0 / (span as f64 + 1.0);
    let mut prev = None;
    for value in values {
        match (prev, value) {
            (_, None) => out.push(None),
            (None, Some(current)) => {
                prev = Some(*current);
                out.push(prev);
            }
            (Some(previous), Some(current)) => {
                let next = alpha * current + (1.0 - alpha) * previous;
                prev = Some(next);
                out.push(prev);
            }
        }
    }
    out
}

fn kdj(
    highs: &[f64],
    lows: &[f64],
    closes: &[f64],
    n: usize,
    m1: usize,
    m2: usize,
) -> (Vec<Option<f64>>, Vec<Option<f64>>, Vec<Option<f64>>) {
    let mut rsv = Vec::with_capacity(closes.len());
    for idx in 0..closes.len() {
        let start = idx.saturating_sub(n.saturating_sub(1));
        let highest = highs[start..=idx]
            .iter()
            .copied()
            .fold(f64::NEG_INFINITY, f64::max);
        let lowest = lows[start..=idx]
            .iter()
            .copied()
            .fold(f64::INFINITY, f64::min);
        let value = if (highest - lowest).abs() < f64::EPSILON {
            50.0
        } else {
            (closes[idx] - lowest) / (highest - lowest) * 100.0
        };
        rsv.push(Some(value));
    }

    let k = smooth(&rsv, m1);
    let d = smooth(&k, m2);
    let j = k
        .iter()
        .zip(d.iter())
        .map(|(k_value, d_value)| match (k_value, d_value) {
            (Some(kv), Some(dv)) => Some(3.0 * kv - 2.0 * dv),
            _ => None,
        })
        .collect();

    (k, d, j)
}

fn smooth(values: &[Option<f64>], period: usize) -> Vec<Option<f64>> {
    let alpha = 1.0 / period as f64;
    let mut out = Vec::with_capacity(values.len());
    let mut prev = 50.0;
    let mut seeded = false;
    for value in values {
        match value {
            Some(current) => {
                if !seeded {
                    prev = *current;
                    seeded = true;
                } else {
                    prev = alpha * current + (1.0 - alpha) * prev;
                }
                out.push(Some(prev));
            }
            None => out.push(None),
        }
    }
    out
}

fn rsi(values: &[f64], period: usize) -> Vec<Option<f64>> {
    let mut out = vec![None; values.len()];
    if values.len() <= period {
        return out;
    }

    let mut gains = 0.0;
    let mut losses = 0.0;
    for idx in 1..=period {
        let diff = values[idx] - values[idx - 1];
        if diff >= 0.0 {
            gains += diff;
        } else {
            losses += -diff;
        }
    }

    let mut avg_gain = gains / period as f64;
    let mut avg_loss = losses / period as f64;
    out[period] = Some(if avg_loss <= f64::EPSILON {
        100.0
    } else {
        let rs = avg_gain / avg_loss;
        100.0 - 100.0 / (1.0 + rs)
    });

    for idx in period + 1..values.len() {
        let diff = values[idx] - values[idx - 1];
        let gain = diff.max(0.0);
        let loss = (-diff).max(0.0);
        avg_gain = (avg_gain * (period as f64 - 1.0) + gain) / period as f64;
        avg_loss = (avg_loss * (period as f64 - 1.0) + loss) / period as f64;
        out[idx] = Some(if avg_loss <= f64::EPSILON {
            100.0
        } else {
            let rs = avg_gain / avg_loss;
            100.0 - 100.0 / (1.0 + rs)
        });
    }

    out
}

#[cfg(test)]
mod tests {
    use chrono::NaiveDate;

    use crate::patterns::model::{Bar, BarSeries};

    use super::SeriesIndicators;

    #[test]
    fn computes_indicator_lengths() {
        let bars = (0..30)
            .map(|idx| Bar {
                symbol: "000001.SZ".to_string(),
                exchange: "SZ".to_string(),
                time: NaiveDate::from_ymd_opt(2025, 1, 1 + idx).unwrap(),
                open: 10.0 + idx as f64,
                high: 10.5 + idx as f64,
                low: 9.5 + idx as f64,
                close: 10.0 + idx as f64,
                volume: 1000.0 + idx as f64,
                amount: None,
                source: Some("test".to_string()),
            })
            .collect();
        let series = BarSeries::new("000001.SZ".to_string(), "SZ".to_string(), bars);
        let indicators = SeriesIndicators::calculate(&series);
        assert_eq!(indicators.ma5.len(), 30);
        assert!(indicators.ma20[19].is_some());
        assert!(indicators.boll_upper[19].is_some());
    }
}
