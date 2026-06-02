use std::time::Duration;

use anyhow::{anyhow, Context, Result};
use async_nats::ConnectOptions;
use axum::extract::State;
use axum::Json;
use futures_util::StreamExt;
use qmt::data::{QuoteEvent, WholeQuoteStreamRequest};
use qmt::QmtClient;
use serde::{Deserialize, Serialize};
use serde_json::json;
use tokio::task::JoinHandle;
use tonic::transport::Endpoint;
use tracing::{error, info, warn};

use super::app::AppState;
use super::errors::{ok, ApiError, ApiResponse};

const DEFAULT_MARKETS: &[&str] = &["SH", "SZ"];
const DEFAULT_RETRY_SECS: u64 = 3;

#[derive(Default)]
pub struct WholeQuoteSubscription {
    handle: Option<JoinHandle<()>>,
    markets: Vec<String>,
}

#[derive(Debug, Deserialize)]
pub struct WholeQuoteStartRequest {
    pub markets: Option<Vec<String>>,
}

#[derive(Debug, Serialize)]
pub struct WholeQuoteStatusResponse {
    pub enabled: bool,
    pub running: bool,
    pub markets: Vec<String>,
    pub nats_url: String,
    pub token_configured: bool,
    pub mode: &'static str,
    pub subject_pattern: &'static str,
}

pub async fn start_whole_quote_forwarder(
    State(state): State<AppState>,
    req: Option<Json<WholeQuoteStartRequest>>,
) -> Result<Json<ApiResponse>, ApiError> {
    if !state.args.quote_forwarder_enabled {
        return Err(ApiError(anyhow!("whole quote forwarder is disabled by config")));
    }

    let markets = normalize_markets(req.and_then(|Json(body)| body.markets))?;
    let mut subscription = state.whole_quote.lock().await;
    if subscription.is_running() {
        return Err(ApiError(anyhow!(
            "whole quote forwarder already running: markets={}",
            subscription.markets.join(",")
        )));
    }

    let task_state = state.clone();
    let task_markets = markets.clone();
    let handle = tokio::spawn(async move {
        run_forwarder_loop(task_state, task_markets).await;
    });

    subscription.handle = Some(handle);
    subscription.markets = markets.clone();
    info!(
        target: "rstock::whole_quote",
        markets = %markets.join(","),
        "whole quote forwarder started"
    );
    Ok(Json(ok("whole quote forwarder started")))
}

pub async fn stop_whole_quote_forwarder(
    State(state): State<AppState>,
) -> Result<Json<ApiResponse>, ApiError> {
    let mut subscription = state.whole_quote.lock().await;
    let Some(handle) = subscription.handle.take() else {
        return Err(ApiError(anyhow!("whole quote forwarder is not running")));
    };

    handle.abort();
    subscription.markets.clear();
    info!(target: "rstock::whole_quote", "whole quote forwarder stopped");
    Ok(Json(ok("whole quote forwarder stopped")))
}

pub async fn whole_quote_forwarder_status(
    State(state): State<AppState>,
) -> Json<WholeQuoteStatusResponse> {
    let subscription = state.whole_quote.lock().await;
    Json(WholeQuoteStatusResponse {
        enabled: state.args.quote_forwarder_enabled,
        running: subscription.is_running(),
        markets: subscription.markets.clone(),
        nats_url: state.args.nats_url.clone(),
        token_configured: state.args.nats_token.is_some(),
        mode: "core",
        subject_pattern: "stock.tick.{code}.{market}",
    })
}

impl WholeQuoteSubscription {
    fn is_running(&self) -> bool {
        self.handle
            .as_ref()
            .map(|handle| !handle.is_finished())
            .unwrap_or(false)
    }
}

async fn run_forwarder_loop(state: AppState, markets: Vec<String>) {
    loop {
        match run_forwarder_once(&state, &markets).await {
            Ok(()) => warn!(
                target: "rstock::whole_quote",
                markets = %markets.join(","),
                "whole quote stream ended, retrying"
            ),
            Err(err) => error!(
                target: "rstock::whole_quote",
                error = ?err,
                markets = %markets.join(","),
                "whole quote forwarder failed"
            ),
        }
        tokio::time::sleep(Duration::from_secs(DEFAULT_RETRY_SECS)).await;
    }
}

async fn run_forwarder_once(state: &AppState, markets: &[String]) -> Result<()> {
    let client = build_qmt_client(state)?;
    let nats = connect_nats(state).await?;
    let mut stream = client
        .data()
        .stream_whole_quote(WholeQuoteStreamRequest {
            markets: markets.to_vec(),
        })
        .await
        .context("subscribe whole quote failed")?
        .into_inner();

    info!(
        target: "rstock::whole_quote",
        markets = %markets.join(","),
        nats_url = %state.args.nats_url,
        "whole quote stream connected"
    );

    while let Some(item) = stream.next().await {
        let event = item.context("receive whole quote event failed")?;
        if let Some((subject, payload)) = build_tick_message(event)? {
            publish_tick(&nats, &subject, payload).await?;
        }
    }

    Ok(())
}

async fn connect_nats(state: &AppState) -> Result<async_nats::Client> {
    let mut options = ConnectOptions::new();
    if let Some(token) = &state.args.nats_token {
        options = options.token(token.clone());
    }
    options
        .connect(&state.args.nats_url)
        .await
        .with_context(|| format!("connect nats failed: {}", state.args.nats_url))
}

fn build_qmt_client(state: &AppState) -> Result<QmtClient> {
    let endpoint = Endpoint::from_shared(normalize_base_url(&state.args.base_url))?
        .timeout(Duration::from_secs(state.args.timeout));
    QmtClient::from_endpoint_with_authorization(endpoint, state.args.authorization.clone())
        .map_err(|err| anyhow!("failed to initialize qmt grpc client: {err}"))
}

async fn publish_tick(
    nats: &async_nats::Client,
    subject: &str,
    payload: Vec<u8>,
) -> Result<()> {
    nats
        .publish(subject.to_string(), payload.into())
        .await
        .with_context(|| format!("publish tick to nats failed: {subject}"))?;
    Ok(())
}

fn build_tick_message(event: QuoteEvent) -> Result<Option<(String, Vec<u8>)>> {
    let Some(payload) = event.payload else {
        return Ok(None);
    };
    let qmt::data::quote_event::Payload::Tick(tick) = payload else {
        return Ok(None);
    };

    let (code, market) = split_symbol(&event.symbol)
        .ok_or_else(|| anyhow!("unsupported symbol format: {}", event.symbol))?;
    let subject = format!("stock.tick.{code}.{market}");
    let payload = json!({
        "symbol": event.symbol,
        "code": code,
        "market": market,
        "period": event.period,
        "event_time_ms": event.event_time_ms,
        "tick": {
            "time_ms": tick.time_ms,
            "last_price": tick.last_price,
            "open": tick.open,
            "high": tick.high,
            "low": tick.low,
            "last_close": tick.last_close,
            "amount": tick.amount,
            "volume": tick.volume,
            "pvolume": tick.pvolume,
            "open_int": tick.open_int,
            "stock_status": tick.stock_status,
            "last_settlement_price": tick.last_settlement_price,
            "ask_price": tick.ask_price,
            "bid_price": tick.bid_price,
            "ask_vol": tick.ask_vol,
            "bid_vol": tick.bid_vol,
            "transaction_num": tick.transaction_num
        }
    });
    Ok(Some((subject, serde_json::to_vec(&payload)?)))
}

fn normalize_markets(markets: Option<Vec<String>>) -> Result<Vec<String>> {
    let markets = markets
        .unwrap_or_else(|| DEFAULT_MARKETS.iter().map(|item| item.to_string()).collect());
    let normalized = markets
        .into_iter()
        .map(|item| item.trim().to_ascii_uppercase())
        .filter(|item| !item.is_empty())
        .collect::<Vec<_>>();
    if normalized.is_empty() {
        return Err(anyhow!("markets cannot be empty"));
    }
    Ok(normalized)
}

fn split_symbol(symbol: &str) -> Option<(String, String)> {
    let trimmed = symbol.trim();
    if let Some((left, right)) = trimmed.split_once('.') {
        let left = left.trim();
        let right = right.trim();

        if is_market(left) && is_code(right) {
            return Some((right.to_string(), left.to_ascii_uppercase()));
        }
        if is_code(left) && is_market(right) {
            return Some((left.to_string(), right.to_ascii_uppercase()));
        }
    }
    None
}

fn is_market(value: &str) -> bool {
    matches!(value.trim().to_ascii_uppercase().as_str(), "SH" | "SZ" | "BJ")
}

fn is_code(value: &str) -> bool {
    let value = value.trim();
    !value.is_empty() && value.chars().all(|ch| ch.is_ascii_digit())
}

fn normalize_base_url(raw: &str) -> String {
    let trimmed = raw.trim();
    if trimmed.starts_with("http://") || trimmed.starts_with("https://") {
        trimmed.to_string()
    } else {
        format!("http://{trimmed}")
    }
}
