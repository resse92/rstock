use anyhow::Result;

use crate::models::{DailyBar, MinuteBar1m};

#[cfg(feature = "tdx-fallback")]
mod enabled {
    use anyhow::{anyhow, Context, Result};
    use chrono::NaiveDate;
    use tdxrs::net::client::TdxHqClient;
    use tdxrs::protocol::constants::{
        fq_type, KLINE_1MIN, KLINE_DAILY, MARKET_BJ, MARKET_SH, MARKET_SZ,
    };
    use tdxrs::protocol::types::SecurityBar;

    use crate::models::{DailyBar, MinuteBar1m};

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
            match client.get_security_bars(KLINE_DAILY, market, &code, 0, 800, fq_type::NONE) {
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

    pub fn fetch_minute_bars(
        codes: &[String],
        start_date: &str,
        end_date: &str,
    ) -> Result<Vec<MinuteBar1m>> {
        let start = parse_compact_date(start_date)?;
        let end = parse_compact_date(end_date)?;
        let count = estimate_minute_count(start, end)?;
        let client = connect_client()?;
        let mut out = Vec::new();

        for symbol in codes {
            let Some((market, code, exchange)) = parse_symbol(symbol) else {
                continue;
            };
            match client.get_security_bars(KLINE_1MIN, market, &code, 0, count, fq_type::NONE) {
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
            amount: Some(bar.amount),
            adj_factor: None,
            settle: None,
            open_interest: None,
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

    fn estimate_minute_count(start: NaiveDate, end: NaiveDate) -> Result<u16> {
        if end < start {
            return Err(anyhow!("开始日期不能晚于结束日期: {start} > {end}"));
        }
        let days = (end - start).num_days() + 1;
        Ok((days * 320).clamp(320, u16::MAX as i64) as u16)
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
pub fn fetch_minute_bars(
    codes: &[String],
    start_date: &str,
    end_date: &str,
) -> Result<Vec<MinuteBar1m>> {
    enabled::fetch_minute_bars(codes, start_date, end_date)
}

#[cfg(not(feature = "tdx-fallback"))]
pub fn fetch_daily_bars(_: &[String], _: &str, _: &str) -> Result<Vec<DailyBar>> {
    eprintln!("[TDX][daily] tdx-fallback feature 未启用，跳过 TDX 兜底");
    Ok(Vec::new())
}

#[cfg(not(feature = "tdx-fallback"))]
pub fn fetch_minute_bars(_: &[String], _: &str, _: &str) -> Result<Vec<MinuteBar1m>> {
    eprintln!("[TDX][minute] tdx-fallback feature 未启用，跳过 TDX 兜底");
    Ok(Vec::new())
}
