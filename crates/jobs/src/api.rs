use std::collections::BTreeSet;
use std::time::Duration;

use anyhow::{anyhow, Context, Result};
use polars::prelude::DataFrame;
use qmt::common::{AdjustType, QuotePeriod, Status as QmtStatus};
use qmt::data::{KlineHistoryRequest, StockListInSectorRequest};
use qmt::QmtClient;
use tonic::transport::Endpoint;

use crate::kline_frame::qmt_kline_response_to_frame;
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
        let endpoint =
            Endpoint::from_shared(normalize_base_url(&base_url.into()))?.timeout(timeout);
        let client = QmtClient::from_endpoint_with_authorization(endpoint, authorization)
            .map_err(|err| anyhow!("failed to initialize qmt grpc client: {err}"))?;
        Ok(Self { client })
    }

    pub async fn fetch_kline_frame(&self, req: &MarketRequest) -> Result<DataFrame> {
        let response = self.fetch_kline_history(req).await?;
        qmt_kline_response_to_frame(&req.period, response, "qmt")
    }

    async fn fetch_kline_history(
        &self,
        req: &MarketRequest,
    ) -> Result<qmt::data::KlineHistoryResponse> {
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
            .with_context(|| {
                format!(
                    "调用 gRPC GetKlineHistory 失败: symbols={}, period={}, start_date={}, end_date={}, adjust_type={}, fill_data={}",
                    req.stock_codes.join(","),
                    req.period,
                    req.start_date,
                    req.end_date,
                    req.adjust_type,
                    req.fill_data
                )
            })?
            .into_inner();

        ensure_status_ok(response.status.as_ref(), "GetKlineHistory")?;
        Ok(response)
    }

    pub async fn discover_all_stock_codes(&self) -> Result<Vec<String>> {
        let codes = self
            .fetch_sector_stock_codes(HS_A_SHARE_SECTOR_NAME)
            .await
            .with_context(|| format!("调用 GetStockListInSector 失败: {HS_A_SHARE_SECTOR_NAME}"))?;
        let codes: BTreeSet<String> = codes
            .into_iter()
            .filter(|code| is_hsa_a_share(code))
            .collect();
        if codes.is_empty() {
            return Err(anyhow!(
                "GetStockListInSector 未返回任何沪深A股代码: {HS_A_SHARE_SECTOR_NAME}"
            ));
        }
        Ok(codes.into_iter().collect())
    }

    async fn fetch_sector_stock_codes(&self, sector_name: &str) -> Result<Vec<String>> {
        let response = self
            .client
            .data()
            .get_stock_list_in_sector(StockListInSectorRequest {
                sector_name: sector_name.to_string(),
            })
            .await
            .with_context(|| format!("调用 gRPC GetStockListInSector 失败: {sector_name}"))?
            .into_inner();

        ensure_status_ok(response.status.as_ref(), "GetStockListInSector")?;
        Ok(response
            .sector
            .map(|sector| sector.symbols)
            .unwrap_or_default())
    }
}

const HS_A_SHARE_SECTOR_NAME: &str = "沪深A股";

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

fn is_hsa_a_share(code: &str) -> bool {
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
    match exchange.as_deref() {
        Some("SH") => sh_a,
        Some("SZ") => sz_a,
        Some(_) => false,
        None => sh_a || sz_a,
    }
}

#[cfg(test)]
mod tests {
    use super::{is_hsa_a_share, normalize_base_url};

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

    #[test]
    fn hs_a_share_filter_accepts_shenzhen_and_shanghai() {
        assert!(is_hsa_a_share("600000.SH"));
        assert!(is_hsa_a_share("000001.SZ"));
    }

    #[test]
    fn hs_a_share_filter_rejects_beijing_exchange() {
        assert!(!is_hsa_a_share("830799.BJ"));
    }
}
