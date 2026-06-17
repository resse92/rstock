use anyhow::{ensure, Result};
use polars::prelude::*;

use super::math::{boll, bull_bear_line, kdj, macd, rolling_mean, rsi, short_trend};
use super::names::{
    boll_lower_column_name, boll_mid_column_name, boll_upper_column_name,
    bull_bear_line_column_name, d_column_name, j_column_name, k_column_name, ma_column_name,
    macd_dea_column_name, macd_dif_column_name, macd_hist_column_name, rsi_column_name,
    short_trend_column_name, volume_ma_column_name,
};
use super::spec::{normalize_specs, IndicatorSpec};

pub fn compute_indicators(mut frame: DataFrame, specs: &[IndicatorSpec]) -> Result<DataFrame> {
    let len = frame.height();
    for spec in normalize_specs(specs) {
        match spec {
            IndicatorSpec::Ma { periods } => {
                let closes = f64_column(&frame, "close", len);
                for period in periods {
                    add_optional_f64_column(
                        &mut frame,
                        &ma_column_name(period),
                        rolling_mean(&closes, period),
                    )?;
                }
            }
            IndicatorSpec::VolumeMa { periods } => {
                let volumes = f64_column(&frame, "volume", len);
                for period in periods {
                    add_optional_f64_column(
                        &mut frame,
                        &volume_ma_column_name(period),
                        rolling_mean(&volumes, period),
                    )?;
                }
            }
            IndicatorSpec::Macd { fast, slow, signal } => {
                ensure!(
                    fast > 0 && slow > 0 && signal > 0,
                    "macd periods must be positive"
                );
                let closes = f64_column(&frame, "close", len);
                let (dif, dea, hist) = macd(&closes, fast, slow, signal);
                add_optional_f64_column(&mut frame, &macd_dif_column_name(fast, slow), dif)?;
                add_optional_f64_column(
                    &mut frame,
                    &macd_dea_column_name(fast, slow, signal),
                    dea,
                )?;
                add_optional_f64_column(
                    &mut frame,
                    &macd_hist_column_name(fast, slow, signal),
                    hist,
                )?;
            }
            IndicatorSpec::Kdj {
                n,
                k_period,
                d_period,
            } => {
                ensure!(
                    n > 0 && k_period > 0 && d_period > 0,
                    "kdj periods must be positive"
                );
                let highs = f64_column(&frame, "high", len);
                let lows = f64_column(&frame, "low", len);
                let closes = f64_column(&frame, "close", len);
                let (k, d, j) = kdj(&highs, &lows, &closes, n, k_period, d_period);
                add_optional_f64_column(&mut frame, &k_column_name(n, k_period, d_period), k)?;
                add_optional_f64_column(&mut frame, &d_column_name(n, k_period, d_period), d)?;
                add_optional_f64_column(&mut frame, &j_column_name(n, k_period, d_period), j)?;
            }
            IndicatorSpec::Boll { period, std_dev } => {
                ensure!(period > 0, "boll period must be positive");
                ensure!(std_dev.is_finite(), "boll std_dev must be finite");
                let closes = f64_column(&frame, "close", len);
                let (mid, upper, lower) = boll(&closes, period, std_dev);
                add_optional_f64_column(&mut frame, &boll_mid_column_name(period), mid)?;
                add_optional_f64_column(
                    &mut frame,
                    &boll_upper_column_name(period, std_dev),
                    upper,
                )?;
                add_optional_f64_column(
                    &mut frame,
                    &boll_lower_column_name(period, std_dev),
                    lower,
                )?;
            }
            IndicatorSpec::Rsi { periods } => {
                let closes = f64_column(&frame, "close", len);
                for period in periods {
                    add_optional_f64_column(
                        &mut frame,
                        &rsi_column_name(period),
                        rsi(&closes, period),
                    )?;
                }
            }
            IndicatorSpec::ShortTrend { fast, smooth } => {
                ensure!(
                    fast > 0 && smooth > 0,
                    "short trend periods must be positive"
                );
                let closes = f64_column(&frame, "close", len);
                add_optional_f64_column(
                    &mut frame,
                    &short_trend_column_name(fast, smooth),
                    short_trend(&closes, fast, smooth),
                )?;
            }
            IndicatorSpec::BullBearLine { periods } => {
                ensure!(
                    periods.iter().all(|period| *period > 0),
                    "bull bear line periods must be positive"
                );
                let closes = f64_column(&frame, "close", len);
                add_optional_f64_column(
                    &mut frame,
                    &bull_bear_line_column_name(periods),
                    bull_bear_line(&closes, periods),
                )?;
            }
        }
    }
    Ok(frame)
}

pub fn compute_indicator(frame: DataFrame, spec: IndicatorSpec) -> Result<DataFrame> {
    compute_indicators(frame, &[spec])
}

fn f64_column(frame: &DataFrame, column: &str, len: usize) -> Vec<f64> {
    frame
        .column(column)
        .ok()
        .and_then(|col| col.f64().ok())
        .map(|values| {
            (0..len)
                .map(|idx| values.get(idx).unwrap_or_default())
                .collect()
        })
        .unwrap_or_else(|| vec![0.0; len])
}

fn add_optional_f64_column(
    frame: &mut DataFrame,
    name: &str,
    values: Vec<Option<f64>>,
) -> Result<()> {
    let series = Series::new(name.into(), values);
    frame.with_column(series)?;
    Ok(())
}
