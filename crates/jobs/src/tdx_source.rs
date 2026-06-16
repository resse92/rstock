use anyhow::Result;
use polars::prelude::DataFrame;
use serde::Serialize;

use crate::kline_frame::{daily_bars_to_frame, minute_bars_to_frame, raw_security_bars_to_frame};
use crate::models::{DailyBar, MinuteBar1m};

#[derive(Debug, Clone, Serialize)]
pub struct RawSecurityBar {
    pub open: f64,
    pub close: f64,
    pub high: f64,
    pub low: f64,
    pub vol: f64,
    pub amount: f64,
    pub year: u32,
    pub month: u32,
    pub day: u32,
    pub hour: u32,
    pub minute: u32,
    pub datetime: String,
}

#[cfg(feature = "tdx-fallback")]
mod enabled {
    use anyhow::{anyhow, Context, Result};
    use chrono::NaiveDate;
    use tdxrs::net::client::TdxHqClient;
    use tdxrs::protocol::constants::{
        fq_type, KLINE_1MIN, KLINE_DAILY, MARKET_BJ, MARKET_SH, MARKET_SZ, MAX_KLINE_COUNT,
    };
    use tdxrs::protocol::types::SecurityBar;

    use crate::models::{DailyBar, MinuteBar1m};
    use crate::tdx_source::RawSecurityBar;

    pub fn fetch_daily_bars(
        codes: &[String],
        start_date: &str,
        end_date: &str,
    ) -> Result<Vec<DailyBar>> {
        let start = parse_compact_date(start_date)?;
        let end = parse_compact_date(end_date)?;
        let client = connect_client()?;
        let mut out = Vec::new();

        for symbol in codes {
            let Some((market, code, exchange)) = parse_symbol(symbol) else {
                continue;
            };
            match fetch_symbol_bars(&client, KLINE_DAILY, market, &code, start) {
                Ok(bars) => {
                    for bar in bars {
                        if let Some(row) = daily_from_tdx_bar(symbol, &exchange, &bar, start, end) {
                            out.push(row);
                        }
                    }
                }
                Err(err) => eprintln!("[TDX][daily] {} fallback failed: {}", symbol, err),
            }
        }

        client.disconnect();
        Ok(out)
    }

    pub fn fetch_daily_security_bars(
        symbol: &str,
        start_date: &str,
        end_date: &str,
    ) -> Result<Vec<RawSecurityBar>> {
        let start = parse_compact_date(start_date)?;
        let end = parse_compact_date(end_date)?;
        let client = connect_client()?;
        let mut out = Vec::new();
        let Some((market, code, _exchange)) = parse_symbol(symbol) else {
            return Ok(out);
        };

        for bar in fetch_symbol_bars(&client, KLINE_DAILY, market, &code, start)? {
            if let Some(date) = bar_date(&bar) {
                if date >= start && date <= end {
                    out.push(raw_from_tdx_bar(&bar));
                }
            }
        }

        client.disconnect();
        Ok(out)
    }

    pub fn fetch_minute_bars(
        codes: &[String],
        start_date: &str,
        end_date: &str,
    ) -> Result<Vec<MinuteBar1m>> {
        let start = parse_compact_date(start_date)?;
        let end = parse_compact_date(end_date)?;
        let client = connect_client()?;
        let mut out = Vec::new();

        for symbol in codes {
            let Some((market, code, exchange)) = parse_symbol(symbol) else {
                continue;
            };
            match fetch_symbol_bars(&client, KLINE_1MIN, market, &code, start) {
                Ok(bars) => {
                    for bar in bars {
                        if let Some(row) = minute_from_tdx_bar(symbol, &exchange, &bar, start, end)
                        {
                            out.push(row);
                        }
                    }
                }
                Err(err) => eprintln!("[TDX][minute] {} fallback failed: {}", symbol, err),
            }
        }

        client.disconnect();
        Ok(out)
    }

    pub fn fetch_minute_security_bars(
        symbol: &str,
        start_date: &str,
        end_date: &str,
    ) -> Result<Vec<RawSecurityBar>> {
        let start = parse_compact_date(start_date)?;
        let end = parse_compact_date(end_date)?;
        let client = connect_client()?;
        let mut out = Vec::new();
        let Some((market, code, _exchange)) = parse_symbol(symbol) else {
            return Ok(out);
        };

        for bar in fetch_symbol_bars(&client, KLINE_1MIN, market, &code, start)? {
            if let Some(date) = bar_date(&bar) {
                if date >= start && date <= end {
                    out.push(raw_from_tdx_bar(&bar));
                }
            }
        }

        client.disconnect();
        Ok(out)
    }

    fn fetch_symbol_bars(
        client: &TdxHqClient,
        category: u8,
        market: u8,
        code: &str,
        start: NaiveDate,
    ) -> Result<Vec<SecurityBar>> {
        let mut out = Vec::new();
        let mut offset = 0u32;

        loop {
            let bars = client.get_security_bars(
                category,
                market,
                code,
                offset,
                MAX_KLINE_COUNT,
                fq_type::NONE,
            )?;
            if bars.is_empty() {
                break;
            }

            let fetched = bars.len() as u32;
            let reached_start = bars
                .first()
                .and_then(bar_date)
                .map(|date| date <= start)
                .unwrap_or(false);

            out.extend(bars);

            if reached_start || fetched < MAX_KLINE_COUNT as u32 {
                break;
            }
            offset += fetched;
        }

        out.sort_by_key(bar_sort_key);
        out.dedup_by_key(|bar| bar_sort_key(bar));
        Ok(out)
    }

    fn connect_client() -> Result<TdxHqClient> {
        let client = TdxHqClient::new();
        client
            .connect_to_any(Some(5.0))
            .map_err(|err| anyhow!("TDX connect failed: {err}"))?;
        Ok(client)
    }

    fn parse_symbol(symbol: &str) -> Option<(u8, String, String)> {
        let (code, exchange) = symbol.rsplit_once('.')?;
        let exchange = exchange.to_ascii_uppercase();
        let market = match exchange.as_str() {
            "SZ" => MARKET_SZ,
            "SH" => MARKET_SH,
            "BJ" => MARKET_BJ,
            _ => return None,
        };
        Some((market, code.to_string(), exchange))
    }

    fn bar_date(bar: &SecurityBar) -> Option<NaiveDate> {
        NaiveDate::from_ymd_opt(bar.year as i32, bar.month, bar.day)
    }

    fn raw_from_tdx_bar(bar: &SecurityBar) -> RawSecurityBar {
        RawSecurityBar {
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
            datetime: bar.datetime.clone(),
        }
    }

    fn bar_sort_key(bar: &SecurityBar) -> (i32, u32, u32, u32, u32) {
        (
            bar.year as i32,
            bar.month,
            bar.day,
            bar.hour.into(),
            bar.minute.into(),
        )
    }

    fn daily_from_tdx_bar(
        symbol: &str,
        exchange: &str,
        bar: &SecurityBar,
        start: NaiveDate,
        end: NaiveDate,
    ) -> Option<DailyBar> {
        let date = NaiveDate::from_ymd_opt(bar.year as i32, bar.month, bar.day)?;
        if date < start || date > end {
            return None;
        }
        Some(DailyBar {
            symbol: symbol.to_string(),
            exchange: exchange.to_string(),
            time: date.format("%Y-%m-%d").to_string(),
            open: Some(bar.open),
            high: Some(bar.high),
            low: Some(bar.low),
            close: Some(bar.close),
            volume: Some(bar.vol),
            amount: Some(bar.amount),
            adj_factor: None,
            settle: None,
            open_interest: None,
            source: Some("tdx".to_string()),
        })
    }

    fn minute_from_tdx_bar(
        symbol: &str,
        exchange: &str,
        bar: &SecurityBar,
        start: NaiveDate,
        end: NaiveDate,
    ) -> Option<MinuteBar1m> {
        let date = NaiveDate::from_ymd_opt(bar.year as i32, bar.month, bar.day)?;
        if date < start || date > end {
            return None;
        }
        Some(MinuteBar1m {
            symbol: symbol.to_string(),
            exchange: exchange.to_string(),
            time: format!(
                "{} {:02}:{:02}:00",
                date.format("%Y-%m-%d"),
                bar.hour,
                bar.minute
            ),
            open: Some(bar.open),
            high: Some(bar.high),
            low: Some(bar.low),
            close: Some(bar.close),
            volume: Some(bar.vol),
            turn_over: Some(bar.amount),
            turn_over_rate: None,
            factor: 1.0,
            source: Some("tdx".to_string()),
        })
    }

    fn parse_compact_date(input: &str) -> Result<NaiveDate> {
        let compact = if input.len() == 10 {
            input.replace('-', "")
        } else {
            input.to_string()
        };
        NaiveDate::parse_from_str(&compact, "%Y%m%d").with_context(|| format!("无效日期: {input}"))
    }
}

#[cfg(feature = "tdx-fallback")]
pub fn fetch_daily_bars(
    codes: &[String],
    start_date: &str,
    end_date: &str,
) -> Result<Vec<DailyBar>> {
    enabled::fetch_daily_bars(codes, start_date, end_date)
}

#[cfg(feature = "tdx-fallback")]
pub fn fetch_daily_security_bars(
    symbol: &str,
    start_date: &str,
    end_date: &str,
) -> Result<Vec<RawSecurityBar>> {
    enabled::fetch_daily_security_bars(symbol, start_date, end_date)
}

#[cfg(feature = "tdx-fallback")]
pub fn fetch_minute_bars(
    codes: &[String],
    start_date: &str,
    end_date: &str,
) -> Result<Vec<MinuteBar1m>> {
    enabled::fetch_minute_bars(codes, start_date, end_date)
}

#[cfg(feature = "tdx-fallback")]
pub fn fetch_minute_security_bars(
    symbol: &str,
    start_date: &str,
    end_date: &str,
) -> Result<Vec<RawSecurityBar>> {
    enabled::fetch_minute_security_bars(symbol, start_date, end_date)
}

#[cfg(not(feature = "tdx-fallback"))]
pub fn fetch_daily_bars(_: &[String], _: &str, _: &str) -> Result<Vec<DailyBar>> {
    eprintln!("[TDX][daily] tdx-fallback feature 未启用，跳过 TDX 兜底");
    Ok(Vec::new())
}

#[cfg(not(feature = "tdx-fallback"))]
pub fn fetch_daily_security_bars(_: &str, _: &str, _: &str) -> Result<Vec<RawSecurityBar>> {
    eprintln!("[TDX][daily] tdx-fallback feature 未启用，跳过 TDX 查询");
    Ok(Vec::new())
}

#[cfg(not(feature = "tdx-fallback"))]
pub fn fetch_minute_bars(_: &[String], _: &str, _: &str) -> Result<Vec<MinuteBar1m>> {
    eprintln!("[TDX][minute] tdx-fallback feature 未启用，跳过 TDX 兜底");
    Ok(Vec::new())
}

#[cfg(not(feature = "tdx-fallback"))]
pub fn fetch_minute_security_bars(_: &str, _: &str, _: &str) -> Result<Vec<RawSecurityBar>> {
    eprintln!("[TDX][minute] tdx-fallback feature 未启用，跳过 TDX 查询");
    Ok(Vec::new())
}

pub fn fetch_daily_bars_frame(
    codes: &[String],
    start_date: &str,
    end_date: &str,
) -> Result<DataFrame> {
    let bars = fetch_daily_bars(codes, start_date, end_date)?;
    daily_bars_to_frame(&bars, "tdx")
}

pub fn fetch_minute_bars_frame(
    codes: &[String],
    start_date: &str,
    end_date: &str,
) -> Result<DataFrame> {
    let bars = fetch_minute_bars(codes, start_date, end_date)?;
    minute_bars_to_frame(&bars, "tdx")
}

pub fn fetch_daily_security_bars_frame(
    symbol: &str,
    start_date: &str,
    end_date: &str,
) -> Result<DataFrame> {
    let bars = fetch_daily_security_bars(symbol, start_date, end_date)?;
    raw_security_bars_to_frame(symbol, "1d", &bars, "tdx")
}

pub fn fetch_minute_security_bars_frame(
    symbol: &str,
    start_date: &str,
    end_date: &str,
) -> Result<DataFrame> {
    let bars = fetch_minute_security_bars(symbol, start_date, end_date)?;
    raw_security_bars_to_frame(symbol, "1m", &bars, "tdx")
}
