use super::names::{
    boll_lower_column_name, boll_mid_column_name, boll_upper_column_name,
    bull_bear_line_column_name, d_column_name, j_column_name, k_column_name, ma_column_name,
    macd_dea_column_name, macd_dif_column_name, macd_hist_column_name, rsi_column_name,
    short_trend_column_name, volume_ma_column_name,
};

#[derive(Debug, Clone, PartialEq)]
pub enum IndicatorSpec {
    Ma {
        periods: Vec<usize>,
    },
    VolumeMa {
        periods: Vec<usize>,
    },
    Macd {
        fast: usize,
        slow: usize,
        signal: usize,
    },
    Kdj {
        n: usize,
        k_period: usize,
        d_period: usize,
    },
    Boll {
        period: usize,
        std_dev: f64,
    },
    Rsi {
        periods: Vec<usize>,
    },
    ShortTrend {
        fast: usize,
        smooth: usize,
    },
    BullBearLine {
        periods: [usize; 4],
    },
}

impl IndicatorSpec {
    pub fn default_pattern_set() -> Vec<Self> {
        vec![
            Self::Ma {
                periods: vec![5, 10, 20, 25, 30],
            },
            Self::VolumeMa {
                periods: vec![5, 10, 60],
            },
            Self::Macd {
                fast: 12,
                slow: 26,
                signal: 9,
            },
            Self::Kdj {
                n: 9,
                k_period: 3,
                d_period: 3,
            },
            Self::Boll {
                period: 20,
                std_dev: 2.0,
            },
            Self::Rsi { periods: vec![14] },
            Self::ShortTrend {
                fast: 10,
                smooth: 10,
            },
            Self::BullBearLine {
                periods: [14, 28, 57, 114],
            },
        ]
    }

    pub fn output_columns(&self) -> Vec<String> {
        match self {
            Self::Ma { periods } => normalized_periods(periods)
                .into_iter()
                .map(ma_column_name)
                .collect(),
            Self::VolumeMa { periods } => normalized_periods(periods)
                .into_iter()
                .map(volume_ma_column_name)
                .collect(),
            Self::Macd { fast, slow, signal } => vec![
                macd_dif_column_name(*fast, *slow),
                macd_dea_column_name(*fast, *slow, *signal),
                macd_hist_column_name(*fast, *slow, *signal),
            ],
            Self::Kdj {
                n,
                k_period,
                d_period,
            } => vec![
                k_column_name(*n, *k_period, *d_period),
                d_column_name(*n, *k_period, *d_period),
                j_column_name(*n, *k_period, *d_period),
            ],
            Self::Boll { period, std_dev } => vec![
                boll_mid_column_name(*period),
                boll_upper_column_name(*period, *std_dev),
                boll_lower_column_name(*period, *std_dev),
            ],
            Self::Rsi { periods } => normalized_periods(periods)
                .into_iter()
                .map(rsi_column_name)
                .collect(),
            Self::ShortTrend { fast, smooth } => vec![short_trend_column_name(*fast, *smooth)],
            Self::BullBearLine { periods } => vec![bull_bear_line_column_name(*periods)],
        }
    }
}

pub(crate) fn normalize_specs(specs: &[IndicatorSpec]) -> Vec<IndicatorSpec> {
    let mut ma_periods = Vec::new();
    let mut volume_ma_periods = Vec::new();
    let mut rsi_periods = Vec::new();
    let mut macd_specs = Vec::new();
    let mut kdj_specs = Vec::new();
    let mut boll_specs = Vec::new();
    let mut short_trend_specs = Vec::new();
    let mut bull_bear_specs = Vec::new();

    for spec in specs {
        match spec {
            IndicatorSpec::Ma { periods } => ma_periods.extend(periods.iter().copied()),
            IndicatorSpec::VolumeMa { periods } => {
                volume_ma_periods.extend(periods.iter().copied())
            }
            IndicatorSpec::Rsi { periods } => rsi_periods.extend(periods.iter().copied()),
            IndicatorSpec::Macd { .. } => push_unique(&mut macd_specs, spec.clone()),
            IndicatorSpec::Kdj { .. } => push_unique(&mut kdj_specs, spec.clone()),
            IndicatorSpec::Boll { .. } => push_unique(&mut boll_specs, spec.clone()),
            IndicatorSpec::ShortTrend { .. } => push_unique(&mut short_trend_specs, spec.clone()),
            IndicatorSpec::BullBearLine { .. } => push_unique(&mut bull_bear_specs, spec.clone()),
        }
    }

    let mut out = Vec::new();
    normalize_periods(&mut ma_periods);
    normalize_periods(&mut volume_ma_periods);
    normalize_periods(&mut rsi_periods);
    if !ma_periods.is_empty() {
        out.push(IndicatorSpec::Ma {
            periods: ma_periods,
        });
    }
    if !volume_ma_periods.is_empty() {
        out.push(IndicatorSpec::VolumeMa {
            periods: volume_ma_periods,
        });
    }
    out.extend(macd_specs);
    out.extend(kdj_specs);
    out.extend(boll_specs);
    if !rsi_periods.is_empty() {
        out.push(IndicatorSpec::Rsi {
            periods: rsi_periods,
        });
    }
    out.extend(short_trend_specs);
    out.extend(bull_bear_specs);
    out
}

fn normalized_periods(periods: &[usize]) -> Vec<usize> {
    let mut periods = periods.to_vec();
    normalize_periods(&mut periods);
    periods
}

fn normalize_periods(periods: &mut Vec<usize>) {
    periods.retain(|period| *period > 0);
    periods.sort_unstable();
    periods.dedup();
}

fn push_unique(specs: &mut Vec<IndicatorSpec>, spec: IndicatorSpec) {
    if !specs.contains(&spec) {
        specs.push(spec);
    }
}
