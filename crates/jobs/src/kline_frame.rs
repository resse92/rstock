use std::collections::{BTreeMap, BTreeSet};

use anyhow::{anyhow, Result};
use chrono::{Datelike, NaiveDate, NaiveDateTime, TimeZone, Timelike, Utc};
use polars::prelude::*;

use crate::models::{date_from_ts_raw, exchange_from_symbol, DailyBar, MinuteBar1m};
use crate::tdx_source::RawSecurityBar;

pub fn empty_kline_frame() -> Result<DataFrame> {
    DataFrame::new(vec![
        Column::new("symbol".into(), Vec::<String>::new()),
        Column::new("exchange".into(), Vec::<String>::new()),
        Column::new("period".into(), Vec::<String>::new()),
        Column::new("time".into(), Vec::<String>::new()),
        Column::new("trade_date".into(), Vec::<String>::new()),
        Column::new("open".into(), Vec::<f64>::new()),
        Column::new("high".into(), Vec::<f64>::new()),
        Column::new("low".into(), Vec::<f64>::new()),
        Column::new("close".into(), Vec::<f64>::new()),
        Column::new("volume".into(), Vec::<f64>::new()),
        Column::new("amount".into(), Vec::<f64>::new()),
        Column::new("turnover_rate".into(), Vec::<Option<f64>>::new()),
        Column::new("factor".into(), Vec::<f64>::new()),
        Column::new("source".into(), Vec::<String>::new()),
    ])
    .map_err(Into::into)
}

pub fn concat_frames(frames: Vec<DataFrame>) -> Result<DataFrame> {
    let mut iter = frames.into_iter();
    let Some(mut out) = iter.next() else {
        return empty_kline_frame();
    };
    for frame in iter {
        out.vstack_mut(&frame)?;
    }
    Ok(out)
}

pub fn frame_symbols(frame: &DataFrame) -> Result<BTreeSet<String>> {
    let symbols = frame.column("symbol")?.str()?;
    let mut out = BTreeSet::new();
    for idx in 0..frame.height() {
        if let Some(symbol) = symbols.get(idx) {
            out.insert(symbol.to_string());
        }
    }
    Ok(out)
}

pub fn partition_frame_by_trade_date_exchange(
    frame: &DataFrame,
) -> Result<BTreeMap<(String, String), DataFrame>> {
    partition_frame_by_string_columns(frame, &["trade_date", "exchange"]).map(|groups| {
        groups
            .into_iter()
            .filter_map(|(key, frame)| {
                if key.len() == 2 {
                    Some(((key[0].clone(), key[1].clone()), frame))
                } else {
                    None
                }
            })
            .collect()
    })
}

pub fn partition_frame_by_string_columns(
    frame: &DataFrame,
    columns: &[&str],
) -> Result<BTreeMap<Vec<String>, DataFrame>> {
    let utf8_columns = columns
        .iter()
        .map(|name| frame.column(name).and_then(|col| col.str()))
        .collect::<PolarsResult<Vec<_>>>()?;
    let mut groups: BTreeMap<Vec<String>, Vec<IdxSize>> = BTreeMap::new();

    for idx in 0..frame.height() {
        let mut key = Vec::with_capacity(columns.len());
        let mut valid = true;
        for column in &utf8_columns {
            let Some(value) = column.get(idx) else {
                valid = false;
                break;
            };
            key.push(value.to_string());
        }
        if valid {
            groups.entry(key).or_default().push(idx as IdxSize);
        }
    }

    let mut out = BTreeMap::new();
    for (key, indices) in groups {
        let idx = IdxCa::from_vec("idx".into(), indices);
        out.insert(key, frame.take(&idx)?);
    }
    Ok(out)
}

pub fn dedup_frame_by_symbol_time(frame: &DataFrame) -> Result<DataFrame> {
    let symbols = frame.column("symbol")?.str()?;
    let times = frame.column("time")?.str()?;
    let exchanges = frame.column("exchange")?.str()?;
    let mut keyed: BTreeMap<(String, String), IdxSize> = BTreeMap::new();

    for idx in 0..frame.height() {
        let Some(symbol) = symbols.get(idx) else {
            continue;
        };
        let Some(time) = times.get(idx) else {
            continue;
        };
        keyed.insert((symbol.to_string(), time.to_string()), idx as IdxSize);
    }

    let mut ordered = keyed.into_iter().collect::<Vec<_>>();
    ordered.sort_by(
        |((left_symbol, left_time), left_idx), ((right_symbol, right_time), right_idx)| {
            left_symbol
                .cmp(right_symbol)
                .then(left_time.cmp(right_time))
                .then_with(|| {
                    let left_exchange = exchanges.get(*left_idx as usize).unwrap_or_default();
                    let right_exchange = exchanges.get(*right_idx as usize).unwrap_or_default();
                    left_exchange.cmp(right_exchange)
                })
        },
    );
    let idx = IdxCa::from_vec(
        "idx".into(),
        ordered.into_iter().map(|(_, idx)| idx).collect(),
    );
    frame.take(&idx).map_err(Into::into)
}

pub fn partition_daily_frame_by_exchange_year_month(
    frame: &DataFrame,
) -> Result<BTreeMap<(String, String, String), DataFrame>> {
    let exchanges = frame.column("exchange")?.str()?;
    let times = frame.column("time")?.str()?;
    let mut groups: BTreeMap<(String, String, String), Vec<IdxSize>> = BTreeMap::new();

    for idx in 0..frame.height() {
        let Some(exchange) = exchanges.get(idx) else {
            continue;
        };
        let Some(time) = times.get(idx) else {
            continue;
        };
        if time.len() < 7 {
            continue;
        }
        groups
            .entry((
                exchange.to_string(),
                time[0..4].to_string(),
                time[5..7].to_string(),
            ))
            .or_default()
            .push(idx as IdxSize);
    }

    let mut out = BTreeMap::new();
    for (key, indices) in groups {
        let idx = IdxCa::from_vec("idx".into(), indices);
        out.insert(key, frame.take(&idx)?);
    }
    Ok(out)
}

pub fn qmt_kline_response_to_frame(
    period: &str,
    response: qmt::data::KlineHistoryResponse,
    source: &str,
) -> Result<DataFrame> {
    let mut symbols = Vec::new();
    let mut exchanges = Vec::new();
    let mut periods = Vec::new();
    let mut times = Vec::new();
    let mut trade_dates = Vec::new();
    let mut open = Vec::new();
    let mut high = Vec::new();
    let mut low = Vec::new();
    let mut close = Vec::new();
    let mut volume = Vec::new();
    let mut amount = Vec::new();
    let mut turnover_rate = Vec::<Option<f64>>::new();
    let mut factor = Vec::new();
    let mut sources = Vec::new();

    for item in response.items {
        let exchange = exchange_from_symbol(&item.symbol).unwrap_or_default();
        for bar in item.bars {
            let time = format_time_ms(period, bar.time_ms);
            let trade_date = date_from_ts_raw(&time).unwrap_or_else(|| time.clone());
            symbols.push(item.symbol.clone());
            exchanges.push(exchange.clone());
            periods.push(period.to_string());
            times.push(time);
            trade_dates.push(trade_date);
            open.push(bar.open);
            high.push(bar.high);
            low.push(bar.low);
            close.push(bar.close);
            volume.push(bar.volume);
            amount.push(bar.amount);
            turnover_rate.push(None);
            factor.push(1.0);
            sources.push(source.to_string());
        }
    }

    DataFrame::new(vec![
        Column::new("symbol".into(), symbols),
        Column::new("exchange".into(), exchanges),
        Column::new("period".into(), periods),
        Column::new("time".into(), times),
        Column::new("trade_date".into(), trade_dates),
        Column::new("open".into(), open),
        Column::new("high".into(), high),
        Column::new("low".into(), low),
        Column::new("close".into(), close),
        Column::new("volume".into(), volume),
        Column::new("amount".into(), amount),
        Column::new("turnover_rate".into(), turnover_rate),
        Column::new("factor".into(), factor),
        Column::new("source".into(), sources),
    ])
    .map_err(Into::into)
}

pub fn daily_bars_to_frame(bars: &[DailyBar], source: &str) -> Result<DataFrame> {
    let mut symbols = Vec::with_capacity(bars.len());
    let mut exchanges = Vec::with_capacity(bars.len());
    let mut periods = Vec::with_capacity(bars.len());
    let mut times = Vec::with_capacity(bars.len());
    let mut trade_dates = Vec::with_capacity(bars.len());
    let mut open = Vec::with_capacity(bars.len());
    let mut high = Vec::with_capacity(bars.len());
    let mut low = Vec::with_capacity(bars.len());
    let mut close = Vec::with_capacity(bars.len());
    let mut volume = Vec::with_capacity(bars.len());
    let mut amount = Vec::with_capacity(bars.len());
    let mut turnover_rate = Vec::<Option<f64>>::with_capacity(bars.len());
    let mut factor = Vec::with_capacity(bars.len());
    let mut sources = Vec::with_capacity(bars.len());

    for bar in bars {
        symbols.push(bar.symbol.clone());
        exchanges.push(bar.exchange.clone());
        periods.push("1d".to_string());
        times.push(bar.time.clone());
        trade_dates.push(bar.time.clone());
        open.push(bar.open.unwrap_or_default());
        high.push(bar.high.unwrap_or_default());
        low.push(bar.low.unwrap_or_default());
        close.push(bar.close.unwrap_or_default());
        volume.push(bar.volume.unwrap_or_default());
        amount.push(bar.amount.unwrap_or_default());
        turnover_rate.push(None);
        factor.push(bar.adj_factor.unwrap_or(1.0));
        sources.push(bar.source.clone().unwrap_or_else(|| source.to_string()));
    }

    DataFrame::new(vec![
        Column::new("symbol".into(), symbols),
        Column::new("exchange".into(), exchanges),
        Column::new("period".into(), periods),
        Column::new("time".into(), times),
        Column::new("trade_date".into(), trade_dates),
        Column::new("open".into(), open),
        Column::new("high".into(), high),
        Column::new("low".into(), low),
        Column::new("close".into(), close),
        Column::new("volume".into(), volume),
        Column::new("amount".into(), amount),
        Column::new("turnover_rate".into(), turnover_rate),
        Column::new("factor".into(), factor),
        Column::new("source".into(), sources),
    ])
    .map_err(Into::into)
}

pub fn minute_bars_to_frame(bars: &[MinuteBar1m], source: &str) -> Result<DataFrame> {
    let mut symbols = Vec::with_capacity(bars.len());
    let mut exchanges = Vec::with_capacity(bars.len());
    let mut periods = Vec::with_capacity(bars.len());
    let mut times = Vec::with_capacity(bars.len());
    let mut trade_dates = Vec::with_capacity(bars.len());
    let mut open = Vec::with_capacity(bars.len());
    let mut high = Vec::with_capacity(bars.len());
    let mut low = Vec::with_capacity(bars.len());
    let mut close = Vec::with_capacity(bars.len());
    let mut volume = Vec::with_capacity(bars.len());
    let mut amount = Vec::with_capacity(bars.len());
    let mut turnover_rate = Vec::<Option<f64>>::with_capacity(bars.len());
    let mut factor = Vec::with_capacity(bars.len());
    let mut sources = Vec::with_capacity(bars.len());

    for bar in bars {
        symbols.push(bar.symbol.clone());
        exchanges.push(bar.exchange.clone());
        periods.push("1m".to_string());
        times.push(bar.time.clone());
        trade_dates.push(date_from_ts_raw(&bar.time).unwrap_or_default());
        open.push(bar.open.unwrap_or_default());
        high.push(bar.high.unwrap_or_default());
        low.push(bar.low.unwrap_or_default());
        close.push(bar.close.unwrap_or_default());
        volume.push(bar.volume.unwrap_or_default());
        amount.push(bar.turn_over.unwrap_or_default());
        turnover_rate.push(bar.turn_over_rate);
        factor.push(bar.factor);
        sources.push(bar.source.clone().unwrap_or_else(|| source.to_string()));
    }

    DataFrame::new(vec![
        Column::new("symbol".into(), symbols),
        Column::new("exchange".into(), exchanges),
        Column::new("period".into(), periods),
        Column::new("time".into(), times),
        Column::new("trade_date".into(), trade_dates),
        Column::new("open".into(), open),
        Column::new("high".into(), high),
        Column::new("low".into(), low),
        Column::new("close".into(), close),
        Column::new("volume".into(), volume),
        Column::new("amount".into(), amount),
        Column::new("turnover_rate".into(), turnover_rate),
        Column::new("factor".into(), factor),
        Column::new("source".into(), sources),
    ])
    .map_err(Into::into)
}

pub fn raw_security_bars_to_frame(
    symbol: &str,
    period: &str,
    bars: &[RawSecurityBar],
    source: &str,
) -> Result<DataFrame> {
    let exchange = exchange_from_symbol(symbol).unwrap_or_default();
    let mut symbols = Vec::with_capacity(bars.len());
    let mut exchanges = Vec::with_capacity(bars.len());
    let mut periods = Vec::with_capacity(bars.len());
    let mut times = Vec::with_capacity(bars.len());
    let mut trade_dates = Vec::with_capacity(bars.len());
    let mut open = Vec::with_capacity(bars.len());
    let mut high = Vec::with_capacity(bars.len());
    let mut low = Vec::with_capacity(bars.len());
    let mut close = Vec::with_capacity(bars.len());
    let mut volume = Vec::with_capacity(bars.len());
    let mut amount = Vec::with_capacity(bars.len());
    let mut turnover_rate = Vec::<Option<f64>>::with_capacity(bars.len());
    let mut factor = Vec::with_capacity(bars.len());
    let mut sources = Vec::with_capacity(bars.len());

    for bar in bars {
        symbols.push(symbol.to_string());
        exchanges.push(exchange.clone());
        periods.push(period.to_string());
        times.push(bar.datetime.clone());
        trade_dates.push(date_from_ts_raw(&bar.datetime).unwrap_or_default());
        open.push(bar.open);
        high.push(bar.high);
        low.push(bar.low);
        close.push(bar.close);
        volume.push(bar.vol);
        amount.push(bar.amount);
        turnover_rate.push(None);
        factor.push(1.0);
        sources.push(source.to_string());
    }

    DataFrame::new(vec![
        Column::new("symbol".into(), symbols),
        Column::new("exchange".into(), exchanges),
        Column::new("period".into(), periods),
        Column::new("time".into(), times),
        Column::new("trade_date".into(), trade_dates),
        Column::new("open".into(), open),
        Column::new("high".into(), high),
        Column::new("low".into(), low),
        Column::new("close".into(), close),
        Column::new("volume".into(), volume),
        Column::new("amount".into(), amount),
        Column::new("turnover_rate".into(), turnover_rate),
        Column::new("factor".into(), factor),
        Column::new("source".into(), sources),
    ])
    .map_err(Into::into)
}

pub fn daily_bars_from_frame(frame: &DataFrame) -> Result<Vec<DailyBar>> {
    let symbols = frame.column("symbol")?.str()?;
    let exchanges = frame.column("exchange")?.str()?;
    let times = frame.column("time")?.str()?;
    let open = frame.column("open")?.f64()?;
    let high = frame.column("high")?.f64()?;
    let low = frame.column("low")?.f64()?;
    let close = frame.column("close")?.f64()?;
    let volume = frame.column("volume")?.f64()?;
    let amount = frame.column("amount")?.f64()?;
    let factor = frame.column("factor")?.f64()?;
    let sources = frame.column("source")?.str()?;

    let mut out = Vec::with_capacity(frame.height());
    for idx in 0..frame.height() {
        let Some(time) = times.get(idx) else {
            continue;
        };
        let date = normalize_date(time)?;
        out.push(DailyBar {
            symbol: symbols.get(idx).unwrap_or_default().to_string(),
            exchange: exchanges.get(idx).unwrap_or_default().to_string(),
            time: date,
            open: open.get(idx),
            high: high.get(idx),
            low: low.get(idx),
            close: close.get(idx),
            volume: volume.get(idx),
            amount: amount.get(idx),
            adj_factor: factor.get(idx),
            settle: None,
            open_interest: None,
            source: sources.get(idx).map(str::to_string),
        });
    }
    Ok(out)
}

pub fn minute_bars_from_frame(frame: &DataFrame) -> Result<Vec<MinuteBar1m>> {
    let symbols = frame.column("symbol")?.str()?;
    let exchanges = frame.column("exchange")?.str()?;
    let times = frame.column("time")?.str()?;
    let open = frame.column("open")?.f64()?;
    let high = frame.column("high")?.f64()?;
    let low = frame.column("low")?.f64()?;
    let close = frame.column("close")?.f64()?;
    let volume = frame.column("volume")?.f64()?;
    let amount = frame.column("amount")?.f64()?;
    let turnover_rate = frame.column("turnover_rate")?.f64()?;
    let factor = frame.column("factor")?.f64()?;
    let sources = frame.column("source")?.str()?;

    let mut out = Vec::with_capacity(frame.height());
    for idx in 0..frame.height() {
        let Some(time) = times.get(idx) else {
            continue;
        };
        out.push(MinuteBar1m {
            symbol: symbols.get(idx).unwrap_or_default().to_string(),
            exchange: exchanges.get(idx).unwrap_or_default().to_string(),
            time: time.to_string(),
            open: open.get(idx),
            high: high.get(idx),
            low: low.get(idx),
            close: close.get(idx),
            volume: volume.get(idx),
            turn_over: amount.get(idx),
            turn_over_rate: turnover_rate.get(idx),
            factor: factor.get(idx).unwrap_or(1.0),
            source: sources.get(idx).map(str::to_string),
        });
    }
    Ok(out)
}

pub fn raw_security_bars_from_frame(frame: &DataFrame) -> Result<Vec<RawSecurityBar>> {
    let times = frame.column("time")?.str()?;
    let open = frame.column("open")?.f64()?;
    let high = frame.column("high")?.f64()?;
    let low = frame.column("low")?.f64()?;
    let close = frame.column("close")?.f64()?;
    let volume = frame.column("volume")?.f64()?;
    let amount = frame.column("amount")?.f64()?;

    let mut out = Vec::with_capacity(frame.height());
    for idx in 0..frame.height() {
        let Some(datetime) = times.get(idx) else {
            continue;
        };
        let (year, month, day, hour, minute) = parse_datetime_parts(datetime)?;
        out.push(RawSecurityBar {
            open: open.get(idx).unwrap_or_default(),
            close: close.get(idx).unwrap_or_default(),
            high: high.get(idx).unwrap_or_default(),
            low: low.get(idx).unwrap_or_default(),
            vol: volume.get(idx).unwrap_or_default(),
            amount: amount.get(idx).unwrap_or_default(),
            year,
            month,
            day,
            hour,
            minute,
            datetime: datetime.to_string(),
        });
    }
    Ok(out)
}

fn normalize_date(input: &str) -> Result<String> {
    if let Ok(date) = NaiveDate::parse_from_str(input, "%Y-%m-%d") {
        return Ok(date.format("%Y-%m-%d").to_string());
    }
    if let Ok(dt) = NaiveDateTime::parse_from_str(input, "%Y-%m-%d %H:%M:%S") {
        return Ok(dt.date().format("%Y-%m-%d").to_string());
    }
    Err(anyhow!("invalid date value: {input}"))
}

fn parse_datetime_parts(input: &str) -> Result<(u32, u32, u32, u32, u32)> {
    if let Ok(date) = NaiveDate::parse_from_str(input, "%Y-%m-%d") {
        return Ok((date.year() as u32, date.month(), date.day(), 0, 0));
    }
    if let Ok(dt) = NaiveDateTime::parse_from_str(input, "%Y-%m-%d %H:%M:%S") {
        return Ok((
            dt.year() as u32,
            dt.month(),
            dt.day(),
            dt.hour(),
            dt.minute(),
        ));
    }
    Err(anyhow!("invalid datetime value: {input}"))
}

fn format_time_ms(period: &str, time_ms: i64) -> String {
    if let Some(dt) = Utc.timestamp_millis_opt(time_ms).single() {
        let local = dt.with_timezone(
            &chrono::FixedOffset::east_opt(8 * 3600).expect("valid china timezone offset"),
        );
        match period.trim().to_ascii_lowercase().as_str() {
            "1d" | "1w" | "1mon" | "1mo" | "1q" | "1hy" | "1y" => {
                local.format("%Y-%m-%d").to_string()
            }
            _ => local.format("%Y-%m-%d %H:%M:%S").to_string(),
        }
    } else {
        time_ms.to_string()
    }
}
