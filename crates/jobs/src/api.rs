use std::collections::BTreeSet;
use std::time::Duration;

use anyhow::{anyhow, Context, Result};
use chrono::{TimeZone, Utc};
use qmt::common::{AdjustType, QuotePeriod, Status as QmtStatus};
use qmt::data::{KlineHistoryRequest, SectorListResponse};
use qmt::QmtClient;
use serde_json::{json, Map, Value};
use tonic::transport::Endpoint;

use crate::models::MarketRequest;

#[derive(Debug, Clone)]
pub struct ApiClient {
    client: QmtClient,
}

impl ApiClient {
    pub fn new(
        base_url: impl Into<String>,
        authorization: Option<String>,
        timeout: Duration,
    ) -> Result<Self> {
        let endpoint = Endpoint::from_shared(normalize_base_url(&base_url.into()))?.timeout(timeout);
        let client = QmtClient::from_endpoint_with_authorization(endpoint, authorization)
            .map_err(|err| anyhow!("failed to initialize qmt grpc client: {err}"))?;
        Ok(Self { client })
    }

    pub async fn fetch_market_batch(&self, req: &MarketRequest) -> Result<Value> {
        let request = KlineHistoryRequest {
            symbols: req.stock_codes.clone(),
            period: map_quote_period(&req.period)? as i32,
            start_time: req.start_date.clone(),
            end_time: req.end_date.clone(),
            fields: Vec::new(),
            adjust_type: map_adjust_type(&req.adjust_type)? as i32,
            fill_data: req.fill_data,
        };

        let response = self
            .client
            .data()
            .get_kline_history(request)
            .await
            .context("调用 gRPC GetKlineHistory 失败")?
            .into_inner();

        ensure_status_ok(response.status.as_ref(), "GetKlineHistory")?;
        Ok(kline_response_to_json(&req.period, response))
    }

    pub async fn fetch_sectors(&self) -> Result<Value> {
        let response = self
            .client
            .data()
            .get_sector_list(())
            .await
            .context("调用 gRPC GetSectorList 失败")?
            .into_inner();

        ensure_status_ok(response.status.as_ref(), "GetSectorList")?;
        Ok(sector_response_to_json(response))
    }

    pub async fn discover_all_stock_codes(&self) -> Result<Vec<String>> {
        let v = self.fetch_sectors().await.context("调用 GetSectorList 失败")?;
        let codes: BTreeSet<String> = extract_stock_list(&v)
            .into_iter()
            .filter(|code| is_hsba_a_share(code))
            .collect();
        if codes.is_empty() {
            return Err(anyhow!("GetSectorList 未返回任何沪深京A股代码"));
        }
        Ok(codes.into_iter().collect())
    }
}

fn ensure_status_ok(status: Option<&QmtStatus>, action: &str) -> Result<()> {
    match status {
        None => Ok(()),
        Some(status) if status.code == 0 => Ok(()),
        Some(status) => Err(anyhow!(
            "{action} failed: code={}, message={}",
            status.code,
            status.message
        )),
    }
}

fn normalize_base_url(raw: &str) -> String {
    let trimmed = raw.trim();
    if trimmed.starts_with("http://") || trimmed.starts_with("https://") {
        trimmed.to_string()
    } else {
        format!("http://{trimmed}")
    }
}

fn map_quote_period(period: &str) -> Result<QuotePeriod> {
    match period.trim().to_ascii_lowercase().as_str() {
        "tick" => Ok(QuotePeriod::Tick),
        "1m" => Ok(QuotePeriod::QuotePeriod1m),
        "5m" => Ok(QuotePeriod::QuotePeriod5m),
        "15m" => Ok(QuotePeriod::QuotePeriod15m),
        "30m" => Ok(QuotePeriod::QuotePeriod30m),
        "1h" => Ok(QuotePeriod::QuotePeriod1h),
        "1d" => Ok(QuotePeriod::QuotePeriod1d),
        "1w" => Ok(QuotePeriod::QuotePeriod1w),
        "1mon" | "1mo" => Ok(QuotePeriod::QuotePeriod1mon),
        "1q" => Ok(QuotePeriod::QuotePeriod1q),
        "1hy" => Ok(QuotePeriod::QuotePeriod1hy),
        "1y" => Ok(QuotePeriod::QuotePeriod1y),
        other => Err(anyhow!("unsupported qmt period: {other}")),
    }
}

fn map_adjust_type(adjust_type: &str) -> Result<AdjustType> {
    match adjust_type.trim().to_ascii_lowercase().as_str() {
        "" | "none" => Ok(AdjustType::None),
        "front" | "qfq" => Ok(AdjustType::Front),
        "back" | "hfq" => Ok(AdjustType::Back),
        "front_ratio" => Ok(AdjustType::FrontRatio),
        "back_ratio" => Ok(AdjustType::BackRatio),
        other => Err(anyhow!("unsupported qmt adjust_type: {other}")),
    }
}

fn kline_response_to_json(period: &str, response: qmt::data::KlineHistoryResponse) -> Value {
    let mut root = Map::new();
    for item in response.items {
        let bars = item
            .bars
            .into_iter()
            .map(|bar| {
                json!({
                    "time": format_time_ms(period, bar.time_ms),
                    "open": bar.open,
                    "high": bar.high,
                    "low": bar.low,
                    "close": bar.close,
                    "volume": bar.volume,
                    "amount": bar.amount,
                    "settle": bar.settle,
                    "openInterest": bar.open_interest,
                    "preClose": bar.pre_close,
                    "suspendFlag": bar.suspend_flag,
                })
            })
            .collect::<Vec<_>>();
        root.insert(item.symbol, Value::Array(bars));
    }
    Value::Object(root)
}

fn sector_response_to_json(response: SectorListResponse) -> Value {
    Value::Array(
        response
            .sectors
            .into_iter()
            .map(|sector| {
                json!({
                    "sector_name": sector.sector_name,
                    "stock_list": sector.symbols,
                })
            })
            .collect(),
    )
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

fn is_hsba_a_share(code: &str) -> bool {
    let trimmed = code.trim();
    if trimmed.is_empty() {
        return false;
    }

    let mut exchange = None;
    let mut symbol = trimmed;
    if let Some((left, right)) = trimmed.rsplit_once('.') {
        symbol = left.trim();
        exchange = Some(right.trim().to_ascii_uppercase());
    }

    let digits: String = symbol.chars().filter(|c| c.is_ascii_digit()).collect();
    if digits.len() < 6 {
        return false;
    }
    let d6 = &digits[..6];

    let sh_a = d6.starts_with("600")
        || d6.starts_with("601")
        || d6.starts_with("603")
        || d6.starts_with("605")
        || d6.starts_with("688")
        || d6.starts_with("689");
    let sz_a = d6.starts_with("000")
        || d6.starts_with("001")
        || d6.starts_with("002")
        || d6.starts_with("003")
        || d6.starts_with("300")
        || d6.starts_with("301");
    let bj_a = d6.starts_with("430")
        || d6.starts_with("440")
        || d6.starts_with("830")
        || d6.starts_with("831")
        || d6.starts_with("832")
        || d6.starts_with("833")
        || d6.starts_with("834")
        || d6.starts_with("835")
        || d6.starts_with("836")
        || d6.starts_with("837")
        || d6.starts_with("838")
        || d6.starts_with("839")
        || d6.starts_with("870")
        || d6.starts_with("871")
        || d6.starts_with("872")
        || d6.starts_with("873")
        || d6.starts_with("874")
        || d6.starts_with("875")
        || d6.starts_with("876")
        || d6.starts_with("877")
        || d6.starts_with("878")
        || d6.starts_with("879")
        || d6.starts_with("880")
        || d6.starts_with("881")
        || d6.starts_with("882")
        || d6.starts_with("883")
        || d6.starts_with("884")
        || d6.starts_with("885")
        || d6.starts_with("886")
        || d6.starts_with("887")
        || d6.starts_with("888")
        || d6.starts_with("920");

    match exchange.as_deref() {
        Some("SH") => sh_a,
        Some("SZ") => sz_a,
        Some("BJ") => bj_a,
        Some(_) => false,
        None => sh_a || sz_a || bj_a,
    }
}

fn extract_stock_list(v: &Value) -> Vec<String> {
    match v {
        Value::Null => vec![],
        Value::Array(arr) => {
            let mut out = Vec::new();
            for item in arr {
                match item {
                    Value::String(s) => out.push(s.to_string()),
                    Value::Object(obj) => {
                        if let Some(Value::Array(codes)) = obj.get("stock_list") {
                            for c in codes {
                                if let Some(s) = c.as_str() {
                                    out.push(s.to_string());
                                }
                            }
                        } else if let Some(s) = obj.get("stock_code").and_then(|x| x.as_str()) {
                            out.push(s.to_string());
                        }
                    }
                    _ => {}
                }
            }
            out
        }
        Value::Object(obj) => {
            if let Some(Value::Array(codes)) = obj.get("stock_list") {
                return codes
                    .iter()
                    .filter_map(|x| x.as_str().map(|s| s.to_string()))
                    .collect();
            }
            for key in ["data", "result", "items"] {
                if let Some(nested) = obj.get(key) {
                    let inner = extract_stock_list(nested);
                    if !inner.is_empty() {
                        return inner;
                    }
                }
            }
            vec![]
        }
        _ => vec![],
    }
}

#[cfg(test)]
mod tests {
    use super::normalize_base_url;

    #[test]
    fn normalize_base_url_adds_http_scheme_when_missing() {
        assert_eq!(
            normalize_base_url("103.85.227.158:40003"),
            "http://103.85.227.158:40003"
        );
    }

    #[test]
    fn normalize_base_url_preserves_existing_scheme() {
        assert_eq!(
            normalize_base_url("https://103.85.227.158:40003"),
            "https://103.85.227.158:40003"
        );
    }
}
