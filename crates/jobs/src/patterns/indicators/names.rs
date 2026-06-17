pub fn ma_column_name(period: usize) -> String {
    format!("ma_{period}")
}

pub fn volume_ma_column_name(period: usize) -> String {
    format!("volume_ma_{period}")
}

pub fn macd_dif_column_name(fast: usize, slow: usize) -> String {
    format!("macd_dif_{fast}_{slow}")
}

pub fn macd_dea_column_name(fast: usize, slow: usize, signal: usize) -> String {
    format!("macd_dea_{fast}_{slow}_{signal}")
}

pub fn macd_hist_column_name(fast: usize, slow: usize, signal: usize) -> String {
    format!("macd_hist_{fast}_{slow}_{signal}")
}

pub fn k_column_name(n: usize, k_period: usize, d_period: usize) -> String {
    format!("k_{n}_{k_period}_{d_period}")
}

pub fn d_column_name(n: usize, k_period: usize, d_period: usize) -> String {
    format!("d_{n}_{k_period}_{d_period}")
}

pub fn j_column_name(n: usize, k_period: usize, d_period: usize) -> String {
    format!("j_{n}_{k_period}_{d_period}")
}

pub fn boll_mid_column_name(period: usize) -> String {
    format!("boll_mid_{period}")
}

pub fn boll_upper_column_name(period: usize, std_dev: f64) -> String {
    format!("boll_upper_{period}_{}", format_number(std_dev))
}

pub fn boll_lower_column_name(period: usize, std_dev: f64) -> String {
    format!("boll_lower_{period}_{}", format_number(std_dev))
}

pub fn rsi_column_name(period: usize) -> String {
    format!("rsi_{period}")
}

pub fn short_trend_column_name(fast: usize, smooth: usize) -> String {
    format!("short_trend_{fast}_{smooth}")
}

pub fn bull_bear_line_column_name(periods: [usize; 4]) -> String {
    format!(
        "bull_bear_line_{}_{}_{}_{}",
        periods[0], periods[1], periods[2], periods[3]
    )
}

fn format_number(value: f64) -> String {
    if value.fract().abs() < f64::EPSILON {
        format!("{value:.0}")
    } else {
        value.to_string().replace('.', "_")
    }
}
