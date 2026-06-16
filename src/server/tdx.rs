use std::time::Duration;

use anyhow::{anyhow, Result};
use axum::extract::{Query, State};
use axum::Json;
use chrono::{NaiveDate, NaiveDateTime};
use jobs::api::ApiClient;
use jobs::kline_frame::{minute_bars_from_frame, raw_security_bars_from_frame};
use jobs::models::{date_from_ts_raw, MarketRequest, MinuteBar1m};
use jobs::tdx_source;
use serde::{Deserialize, Serialize};
use tdxrs::protocol::constants::KLINE_DAILY;
use tdxrs::protocol::types::SecurityBar;

use super::app::AppState;
use super::errors::ApiError;

#[derive(Debug, Deserialize)]
pub struct TdxKlineQuery {
    pub symbol: String,
    pub start_date: Option<String>,
    pub end_date: Option<String>,
    pub start_time: Option<String>,
    pub end_time: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct TdxMinuteBarResponse {
    pub symbol: String,
    pub exchange: String,
    pub time: String,
    pub trade_date: String,
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
    pub factor: f64,
    pub volume: f64,
    pub turnover: f64,
    pub turnover_rate: Option<f64>,
    pub is_paused: Option<f64>,
}

pub async fn get_tdx_daily_kline(
    Query(query): Query<TdxKlineQuery>,
) -> Result<Json<Vec<SecurityBar>>, ApiError> {
    let query = normalize_daily_query(query)?;
    let bars = fetch_security_bars(&query, KLINE_DAILY)?;
    Ok(Json(bars))
}

pub async fn get_tdx_minute_kline(
    State(state): State<AppState>,
    Query(query): Query<TdxKlineQuery>,
) -> Result<Json<Vec<TdxMinuteBarResponse>>, ApiError> {
    let query = normalize_minute_query(query)?;
    let exchange = parse_exchange(&query.symbol)?;
    let bars = fetch_qmt_minute_bars(&state, &query).await?;
    let bars = bars
        .into_iter()
        .map(|bar| map_minute_bar(&query.symbol, &exchange, bar))
        .collect();
    Ok(Json(bars))
}

fn fetch_security_bars(query: &TdxKlineQuery, category: u8) -> Result<Vec<SecurityBar>> {
    if category != KLINE_DAILY {
        return Err(anyhow!("unsupported tdx category: {category}"));
    }
    let start = parse_compact_date(
        query
            .start_date
            .as_deref()
            .ok_or_else(|| anyhow!("start_date is required"))?,
    )?;
    let end = parse_compact_date(
        query
            .end_date
            .as_deref()
            .ok_or_else(|| anyhow!("end_date is required"))?,
    )?;
    let frame = tdx_source::fetch_daily_security_bars_frame(
        &query.symbol,
        &start.format("%Y%m%d").to_string(),
        &end.format("%Y%m%d").to_string(),
    )?;
    let rows = raw_security_bars_from_frame(&frame)?;
    Ok(rows
        .into_iter()
        .map(|bar| SecurityBar {
            open: bar.open,
            close: bar.close,
            high: bar.high,
            low: bar.low,
            vol: bar.vol,
            amount: bar.amount,
            year: bar.year,
            month: bar.month,
            day: bar.day,
            hour: bar.hour,
            minute: bar.minute,
            datetime: bar.datetime,
        })
        .collect())
}

async fn fetch_qmt_minute_bars(
    state: &AppState,
    query: &TdxKlineQuery,
) -> Result<Vec<MinuteBar1m>> {
    let start_time = query
        .start_time
        .as_ref()
        .ok_or_else(|| anyhow!("start_time is required for minute query"))?;
    let end_time = query
        .end_time
        .as_ref()
        .ok_or_else(|| anyhow!("end_time is required for minute query"))?;
    let api = ApiClient::new(
        state.args.base_url.clone(),
        state.args.authorization.clone(),
        Duration::from_secs(state.args.timeout),
    )?;
    let req = MarketRequest::new(
        vec![query.symbol.clone()],
        "1m",
        start_time.clone(),
        end_time.clone(),
        "none",
    );
    let frame = api.fetch_kline_frame(&req).await?;
    let mut rows = minute_bars_from_frame(&frame)?;
    rows.retain(|row| row.symbol == query.symbol);
    rows.sort_by(|a, b| a.time.cmp(&b.time));
    Ok(rows)
}

fn normalize_daily_query(query: TdxKlineQuery) -> Result<TdxKlineQuery> {
    let symbol = normalize_symbol(&query.symbol)?;
    let start_input = query
        .start_date
        .as_deref()
        .ok_or_else(|| anyhow!("start_date is required"))?;
    let end_input = query
        .end_date
        .as_deref()
        .ok_or_else(|| anyhow!("end_date is required"))?;
    let start_date = normalize_compact_date(start_input)?;
    let end_date = normalize_compact_date(end_input)?;
    if start_date > end_date {
        return Err(anyhow!(
            "start_date must be <= end_date: {} > {}",
            start_date,
            end_date
        ));
    }
    Ok(TdxKlineQuery {
        symbol,
        start_date: Some(start_date),
        end_date: Some(end_date),
        start_time: None,
        end_time: None,
    })
}

fn normalize_minute_query(query: TdxKlineQuery) -> Result<TdxKlineQuery> {
    let symbol = normalize_symbol(&query.symbol)?;

    let (start_date, start_time) = match query.start_time.as_deref() {
        Some(value) => {
            let dt = normalize_compact_datetime(value)?;
            (dt[..8].to_string(), dt)
        }
        None => {
            let date = normalize_compact_date(
                query
                    .start_date
                    .as_deref()
                    .ok_or_else(|| anyhow!("start_date or start_time is required"))?,
            )?;
            let time = format!("{}093000", date);
            (date, time)
        }
    };

    let (end_date, end_time) = match query.end_time.as_deref() {
        Some(value) => {
            let dt = normalize_compact_datetime(value)?;
            (dt[..8].to_string(), dt)
        }
        None => {
            let date = normalize_compact_date(
                query
                    .end_date
                    .as_deref()
                    .ok_or_else(|| anyhow!("end_date or end_time is required"))?,
            )?;
            let time = format!("{}150000", date);
            (date, time)
        }
    };

    if start_time > end_time {
        return Err(anyhow!(
            "start_time must be <= end_time: {} > {}",
            start_time,
            end_time
        ));
    }

    Ok(TdxKlineQuery {
        symbol,
        start_date: Some(start_date),
        end_date: Some(end_date),
        start_time: Some(start_time),
        end_time: Some(end_time),
    })
}

fn normalize_symbol(input: &str) -> Result<String> {
    let trimmed = input.trim();
    let (code, exchange) = trimmed
        .rsplit_once('.')
        .ok_or_else(|| anyhow!("invalid symbol {trimmed}, expected code.exchange"))?;
    if code.len() != 6 || !code.chars().all(|c| c.is_ascii_digit()) {
        return Err(anyhow!("invalid symbol code {code}, expected 6 digits"));
    }
    let exchange = exchange.trim().to_ascii_uppercase();
    match exchange.as_str() {
        "SH" | "SZ" | "BJ" => Ok(format!("{code}.{exchange}")),
        _ => Err(anyhow!("invalid exchange {exchange}, expected SH/SZ/BJ")),
    }
}

fn parse_exchange(symbol: &str) -> Result<String> {
    let (_, exchange) = symbol
        .rsplit_once('.')
        .ok_or_else(|| anyhow!("invalid symbol {symbol}, expected code.exchange"))?;
    Ok(exchange.to_string())
}

fn normalize_compact_date(input: &str) -> Result<String> {
    let trimmed = input.trim();
    if trimmed.is_empty() {
        return Err(anyhow!("date is empty"));
    }
    let compact = if trimmed.len() == 10 {
        trimmed.replace('-', "")
    } else {
        trimmed.to_string()
    };
    chrono::NaiveDate::parse_from_str(&compact, "%Y%m%d")
        .map_err(|err| anyhow!("invalid date {trimmed}: {err}"))?;
    Ok(compact)
}

fn normalize_compact_datetime(input: &str) -> Result<String> {
    let trimmed = input.trim();
    if trimmed.is_empty() {
        return Err(anyhow!("datetime is empty"));
    }

    let compact = match trimmed.len() {
        19 if trimmed.as_bytes().get(4) == Some(&b'-') => trimmed.replace(['-', ':', ' '], ""),
        14 => trimmed.to_string(),
        _ => {
            return Err(anyhow!(
                "invalid datetime {trimmed}, expected YYYYMMDDHHMMSS or YYYY-MM-DD HH:MM:SS"
            ))
        }
    };

    NaiveDateTime::parse_from_str(&compact, "%Y%m%d%H%M%S")
        .map_err(|err| anyhow!("invalid datetime {trimmed}: {err}"))?;
    Ok(compact)
}

fn parse_compact_date(input: &str) -> Result<NaiveDate> {
    NaiveDate::parse_from_str(input, "%Y%m%d").map_err(|err| anyhow!("invalid date {input}: {err}"))
}

fn map_minute_bar(symbol: &str, exchange: &str, bar: MinuteBar1m) -> TdxMinuteBarResponse {
    let time = bar.time;
    TdxMinuteBarResponse {
        symbol: symbol.to_string(),
        exchange: exchange.to_string(),
        trade_date: date_from_ts_raw(&time).unwrap_or_default(),
        time,
        open: bar.open.unwrap_or_default(),
        high: bar.high.unwrap_or_default(),
        low: bar.low.unwrap_or_default(),
        close: bar.close.unwrap_or_default(),
        factor: bar.factor,
        volume: bar.volume.unwrap_or_default(),
        turnover: bar.turn_over.unwrap_or_default(),
        turnover_rate: bar.turn_over_rate,
        is_paused: None,
    }
}
