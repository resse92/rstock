//! Tick response normalization helpers.
//! K-line normalization has been removed in favor of the DataFrame-based pipeline.

use serde_json::{Map, Value};

use crate::models::{exchange_from_symbol, TickQuote};

fn pick<'a>(m: &'a Map<String, Value>, keys: &[&str]) -> Option<&'a Value> {
    for key in keys {
        if let Some(v) = m.get(*key) {
            return Some(v);
        }
    }
    None
}

fn to_opt_f64(v: Option<&Value>) -> Option<f64> {
    match v {
        None => None,
        Some(Value::Null) => None,
        Some(Value::String(s)) if s.trim().is_empty() => None,
        Some(Value::Number(n)) => n.as_f64(),
        Some(Value::String(s)) => s.parse::<f64>().ok(),
        _ => None,
    }
}

fn likely_code(s: &str) -> bool {
    let parts: Vec<&str> = s.split('.').collect();
    if parts.len() != 2 {
        return false;
    }
    parts
        .iter()
        .all(|p| !p.is_empty() && p.chars().all(|ch| ch.is_ascii_alphanumeric() || ch == '_'))
}

fn to_opt_string(v: Option<&Value>) -> Option<String> {
    match v {
        None | Some(Value::Null) => None,
        Some(Value::String(s)) => Some(s.to_string()),
        Some(other) => Some(other.to_string()),
    }
}

fn to_opt_status(v: Option<&Value>) -> Option<String> {
    match v {
        None | Some(Value::Null) => None,
        Some(Value::String(s)) => Some(s.to_string()),
        Some(other) => Some(other.to_string()),
    }
}

fn normalize_tick_row(symbol: &str, obj: &Map<String, Value>) -> Option<TickQuote> {
    let exchange = exchange_from_symbol(symbol)?;
    let time = to_opt_string(pick(obj, &["time", "timestamp", "datetime"]))?;

    Some(TickQuote {
        symbol: symbol.to_string(),
        exchange,
        time,
        last_price: to_opt_f64(pick(obj, &["lastPrice"])),
        open: to_opt_f64(pick(obj, &["open"])),
        high: to_opt_f64(pick(obj, &["high"])),
        low: to_opt_f64(pick(obj, &["low"])),
        last_close: to_opt_f64(pick(obj, &["lastClose"])),
        amount: to_opt_f64(pick(obj, &["amount"])),
        volume: to_opt_f64(pick(obj, &["volume"])),
        pvolume: to_opt_f64(pick(obj, &["pvolume"])),
        stock_status: to_opt_status(pick(obj, &["stockStatus"])),
        open_interest: to_opt_f64(pick(obj, &["openInt", "openInterest"])),
        last_settlement_price: to_opt_f64(pick(obj, &["lastSettlementPrice"])),
        ask_price_1: to_opt_f64(pick(obj, &["askPrice1"])),
        ask_price_2: to_opt_f64(pick(obj, &["askPrice2"])),
        ask_price_3: to_opt_f64(pick(obj, &["askPrice3"])),
        ask_price_4: to_opt_f64(pick(obj, &["askPrice4"])),
        ask_price_5: to_opt_f64(pick(obj, &["askPrice5"])),
        bid_price_1: to_opt_f64(pick(obj, &["bidPrice1"])),
        bid_price_2: to_opt_f64(pick(obj, &["bidPrice2"])),
        bid_price_3: to_opt_f64(pick(obj, &["bidPrice3"])),
        bid_price_4: to_opt_f64(pick(obj, &["bidPrice4"])),
        bid_price_5: to_opt_f64(pick(obj, &["bidPrice5"])),
        ask_vol_1: to_opt_f64(pick(obj, &["askVol1"])),
        ask_vol_2: to_opt_f64(pick(obj, &["askVol2"])),
        ask_vol_3: to_opt_f64(pick(obj, &["askVol3"])),
        ask_vol_4: to_opt_f64(pick(obj, &["askVol4"])),
        ask_vol_5: to_opt_f64(pick(obj, &["askVol5"])),
        bid_vol_1: to_opt_f64(pick(obj, &["bidVol1"])),
        bid_vol_2: to_opt_f64(pick(obj, &["bidVol2"])),
        bid_vol_3: to_opt_f64(pick(obj, &["bidVol3"])),
        bid_vol_4: to_opt_f64(pick(obj, &["bidVol4"])),
        bid_vol_5: to_opt_f64(pick(obj, &["bidVol5"])),
    })
}

fn extract_ticks_for_symbol(symbol: &str, payload: &Value) -> Vec<TickQuote> {
    match payload {
        Value::Array(arr) => arr
            .iter()
            .filter_map(|x| x.as_object())
            .filter_map(|obj| normalize_tick_row(symbol, obj))
            .collect(),
        Value::Object(obj) => {
            for key in ["tick", "ticks", "data", "items", "result"] {
                if let Some(v) = obj.get(key) {
                    let rows = extract_ticks_for_symbol(symbol, v);
                    if !rows.is_empty() {
                        return rows;
                    }
                }
            }
            normalize_tick_row(symbol, obj).into_iter().collect()
        }
        _ => vec![],
    }
}

pub fn normalize_full_tick_response(data: &Value) -> Vec<TickQuote> {
    let mut rows = Vec::new();
    match data {
        Value::Null => {}
        Value::Array(arr) => {
            for item in arr {
                rows.extend(normalize_full_tick_response(item));
            }
        }
        Value::Object(obj) => {
            for key in ["data", "result", "items"] {
                if let Some(v) = obj.get(key) {
                    rows.extend(normalize_full_tick_response(v));
                }
            }
            for (k, v) in obj {
                if ["data", "result", "items"].contains(&k.as_str()) {
                    continue;
                }
                if likely_code(k) {
                    rows.extend(extract_ticks_for_symbol(k, v));
                }
            }
        }
        _ => {}
    }
    rows
}
