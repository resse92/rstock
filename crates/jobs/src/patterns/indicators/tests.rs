use chrono::NaiveDate;
use polars::df;

use crate::patterns::detectors::default_detectors;
use crate::patterns::model::{Bar, BarSeries};

use super::{
    boll_upper_column_name, compute_indicators, k_column_name, ma_column_name,
    macd_hist_column_name, volume_ma_column_name, IndicatorSpec,
};

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
    let indicators =
        compute_indicators(series.frame.clone(), &IndicatorSpec::default_pattern_set()).unwrap();
    assert_eq!(indicators.column(&ma_column_name(5)).unwrap().len(), 30);
    assert_eq!(indicators.height(), series.len());
    assert!(indicators
        .column(&ma_column_name(20))
        .unwrap()
        .f64()
        .unwrap()
        .get(19)
        .is_some());
    assert!(indicators
        .column(&boll_upper_column_name(20, 2.0))
        .unwrap()
        .f64()
        .unwrap()
        .get(19)
        .is_some());
}

#[test]
fn computes_indicator_lengths_when_volume_column_is_missing() {
    let frame = df!(
        "symbol" => ["000001.SZ", "000001.SZ", "000001.SZ"],
        "exchange" => ["SZ", "SZ", "SZ"],
        "time" => ["2025-01-01", "2025-01-02", "2025-01-03"],
        "open" => [10.0, 10.2, 10.4],
        "high" => [10.3, 10.5, 10.7],
        "low" => [9.9, 10.0, 10.2],
        "close" => [10.1, 10.3, 10.6]
    )
    .unwrap();
    let series = BarSeries::from_frame("000001.SZ".to_string(), "SZ".to_string(), frame).unwrap();

    let indicators =
        compute_indicators(series.frame.clone(), &IndicatorSpec::default_pattern_set()).unwrap();

    assert_eq!(indicators.height(), series.len());
    assert_eq!(
        indicators.column(&volume_ma_column_name(5)).unwrap().len(),
        series.len()
    );
    assert_eq!(
        indicators.column(&ma_column_name(5)).unwrap().len(),
        series.len()
    );
}

#[test]
fn computes_only_requested_parameterized_indicators() {
    let frame = df!(
        "symbol" => vec!["000001.SZ"; 30],
        "exchange" => vec!["SZ"; 30],
        "time" => (1..=30).map(|day| format!("2025-01-{day:02}")).collect::<Vec<_>>(),
        "open" => (0..30).map(|idx| 10.0 + idx as f64).collect::<Vec<_>>(),
        "high" => (0..30).map(|idx| 10.5 + idx as f64).collect::<Vec<_>>(),
        "low" => (0..30).map(|idx| 9.5 + idx as f64).collect::<Vec<_>>(),
        "close" => (0..30).map(|idx| 10.0 + idx as f64).collect::<Vec<_>>(),
        "volume" => (0..30).map(|idx| 1000.0 + idx as f64).collect::<Vec<_>>()
    )
    .unwrap();

    let frame = compute_indicators(
        frame,
        &[
            IndicatorSpec::Ma {
                periods: vec![3, 5, 3],
            },
            IndicatorSpec::VolumeMa { periods: vec![2] },
            IndicatorSpec::Macd {
                fast: 12,
                slow: 26,
                signal: 9,
            },
            IndicatorSpec::Kdj {
                n: 9,
                k_period: 3,
                d_period: 3,
            },
        ],
    )
    .unwrap();

    assert!(frame.column(&ma_column_name(3)).is_ok());
    assert!(frame.column(&ma_column_name(5)).is_ok());
    assert!(frame.column(&ma_column_name(10)).is_err());
    assert!(frame.column(&volume_ma_column_name(2)).is_ok());
    assert!(frame.column(&macd_hist_column_name(12, 26, 9)).is_ok());
    assert!(frame.column(&k_column_name(9, 3, 3)).is_ok());
    assert_eq!(
        frame
            .column(&ma_column_name(3))
            .unwrap()
            .f64()
            .unwrap()
            .get(2),
        Some(11.0)
    );
}

#[test]
fn default_detectors_do_not_panic_when_optional_columns_are_missing() {
    let times = (1..=130)
        .map(|day| format!("2025-01-{day:02}"))
        .collect::<Vec<_>>();
    let frame = df!(
        "symbol" => vec!["000001.SZ"; 130],
        "exchange" => vec!["SZ"; 130],
        "time" => times,
        "open" => (0..130).map(|idx| 10.0 + idx as f64 * 0.1).collect::<Vec<_>>(),
        "high" => (0..130).map(|idx| 10.3 + idx as f64 * 0.1).collect::<Vec<_>>(),
        "low" => (0..130).map(|idx| 9.8 + idx as f64 * 0.1).collect::<Vec<_>>(),
        "close" => (0..130).map(|idx| 10.1 + idx as f64 * 0.1).collect::<Vec<_>>()
    )
    .unwrap();
    let series = BarSeries::from_frame("000001.SZ".to_string(), "SZ".to_string(), frame).unwrap();
    let indicators =
        compute_indicators(series.frame.clone(), &IndicatorSpec::default_pattern_set()).unwrap();

    assert_eq!(indicators.height(), series.len());
    for detector in default_detectors() {
        detector.detect(&series, &indicators);
    }
}
