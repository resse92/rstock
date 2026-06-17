pub(crate) fn rolling_mean(values: &[f64], window: usize) -> Vec<Option<f64>> {
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

pub(crate) fn macd(
    values: &[f64],
    fast: usize,
    slow: usize,
    signal: usize,
) -> (Vec<Option<f64>>, Vec<Option<f64>>, Vec<Option<f64>>) {
    let ema_fast = ema(values, fast);
    let ema_slow = ema(values, slow);
    let dif = ema_fast
        .iter()
        .zip(ema_slow.iter())
        .map(|(fast, slow)| match (fast, slow) {
            (Some(f), Some(s)) => Some(f - s),
            _ => None,
        })
        .collect::<Vec<_>>();
    let dea = ema_optional(&dif, signal);
    let hist = dif
        .iter()
        .zip(dea.iter())
        .map(|(d, e)| match (d, e) {
            (Some(dif_value), Some(dea_value)) => Some(dif_value - dea_value),
            _ => None,
        })
        .collect();
    (dif, dea, hist)
}

pub(crate) fn kdj(
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

pub(crate) fn boll(
    values: &[f64],
    period: usize,
    std_dev: f64,
) -> (Vec<Option<f64>>, Vec<Option<f64>>, Vec<Option<f64>>) {
    let mid = rolling_mean(values, period);
    let std = rolling_std(values, period);
    let (upper, lower) = mid
        .iter()
        .zip(std.iter())
        .map(|(mid, std)| match (mid, std) {
            (Some(mid_value), Some(std_value)) => (
                Some(mid_value + std_dev * std_value),
                Some(mid_value - std_dev * std_value),
            ),
            _ => (None, None),
        })
        .unzip();
    (mid, upper, lower)
}

pub(crate) fn rsi(values: &[f64], period: usize) -> Vec<Option<f64>> {
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
    out[period] = Some(rsi_value(avg_gain, avg_loss));

    for idx in period + 1..values.len() {
        let diff = values[idx] - values[idx - 1];
        let gain = diff.max(0.0);
        let loss = (-diff).max(0.0);
        avg_gain = (avg_gain * (period as f64 - 1.0) + gain) / period as f64;
        avg_loss = (avg_loss * (period as f64 - 1.0) + loss) / period as f64;
        out[idx] = Some(rsi_value(avg_gain, avg_loss));
    }

    out
}

pub(crate) fn short_trend(values: &[f64], fast: usize, smooth: usize) -> Vec<Option<f64>> {
    ema_optional(&ema(values, fast), smooth)
}

pub(crate) fn bull_bear_line(values: &[f64], periods: [usize; 4]) -> Vec<Option<f64>> {
    let ma_a = rolling_mean(values, periods[0]);
    let ma_b = rolling_mean(values, periods[1]);
    let ma_c = rolling_mean(values, periods[2]);
    let ma_d = rolling_mean(values, periods[3]);
    (0..values.len())
        .map(|idx| match (ma_a[idx], ma_b[idx], ma_c[idx], ma_d[idx]) {
            (Some(a), Some(b), Some(c), Some(d)) => Some((a + b + c + d) / 4.0),
            _ => None,
        })
        .collect()
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

fn rsi_value(avg_gain: f64, avg_loss: f64) -> f64 {
    if avg_loss <= f64::EPSILON {
        100.0
    } else {
        let rs = avg_gain / avg_loss;
        100.0 - 100.0 / (1.0 + rs)
    }
}
