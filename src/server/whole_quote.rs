use std::time::{Duration, Instant};

use anyhow::{anyhow, Context, Result};
use async_nats::ConnectOptions;
use axum::extract::State;
use axum::Json;
use futures_util::future::try_join_all;
use qmt::common::Status as QmtStatus;
use qmt::data::{FullTickSnapshot, FullTickSnapshotRequest, StockListInSectorRequest};
use qmt::QmtClient;
use serde::{Deserialize, Serialize};
use serde_json::json;
use tokio::task::JoinHandle;
use tokio::time::MissedTickBehavior;
use tonic::transport::Endpoint;
use tracing::{error, info};

use super::app::AppState;
use super::errors::{ok, ApiError, ApiResponse};

const DEFAULT_SECTOR_NAME: &str = "沪深A股";
const SNAPSHOT_INTERVAL_SECS: u64 = 10;
const SNAPSHOT_WORKER_COUNT: usize = 10;

#[derive(Default)]
pub struct WholeQuoteSubscription {
    handle: Option<JoinHandle<()>>,
    sector_name: Option<String>,
    symbols: Vec<String>,
}

#[derive(Debug, Deserialize)]
pub struct WholeQuoteStartRequest {
    pub sector_name: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct WholeQuoteStatusResponse {
    pub enabled: bool,
    pub running: bool,
    pub sector_name: Option<String>,
    pub symbol_count: usize,
    pub snapshot_worker_count: usize,
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

    let sector_name = normalize_sector_name(req.and_then(|Json(body)| body.sector_name));
    let client = build_qmt_client(&state)?;
    let symbols = discover_symbols_by_sector(&client, &sector_name).await?;
    let mut subscription = state.whole_quote.lock().await;
    if subscription.is_running() {
        return Err(ApiError(anyhow!(
            "whole quote forwarder already running: sector_name={}",
            subscription.sector_name.as_deref().unwrap_or_default()
        )));
    }

    let task_state = state.clone();
    let task_symbols = symbols.clone();
    let handle = tokio::spawn(async move {
        run_forwarder_loop(task_state, task_symbols).await;
    });

    subscription.handle = Some(handle);
    subscription.sector_name = Some(sector_name.clone());
    subscription.symbols = symbols.clone();
    info!(
        target: "rstock::whole_quote",
        sector_name = %sector_name,
        symbol_count = symbols.len(),
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
    subscription.sector_name = None;
    subscription.symbols.clear();
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
        sector_name: subscription.sector_name.clone(),
        symbol_count: subscription.symbols.len(),
        snapshot_worker_count: SNAPSHOT_WORKER_COUNT,
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

async fn run_forwarder_loop(state: AppState, symbols: Vec<String>) {
    let mut nats = None;
    let client = match build_qmt_client(&state) {
        Ok(client) => client,
        Err(err) => {
            error!(
                target: "rstock::whole_quote",
                error = ?err,
                symbol_count = symbols.len(),
                "build qmt client failed"
            );
            return;
        }
    };
    let mut interval = tokio::time::interval(Duration::from_secs(SNAPSHOT_INTERVAL_SECS));
    interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
    loop {
        interval.tick().await;
        if nats.is_none() {
            match connect_nats(&state).await {
                Ok(client) => {
                    nats = Some(client);
                }
                Err(err) => {
                    error!(
                        target: "rstock::whole_quote",
                        error = ?err,
                        symbol_count = symbols.len(),
                        "connect nats failed"
                    );
                    continue;
                }
            }
        }

        match run_forwarder_once(&state, &client, nats.as_ref().expect("nats connected"), &symbols)
            .await
        {
            Ok(()) => {}
            Err(err) => error!(
                target: "rstock::whole_quote",
                error = ?err,
                symbol_count = symbols.len(),
                "whole quote forwarder failed"
            ),
        }
        if let Some(client) = &nats {
            if !matches!(
                client.connection_state(),
                async_nats::connection::State::Connected
            ) {
                nats = None;
            }
        }
    }
}

async fn run_forwarder_once(
    state: &AppState,
    client: &QmtClient,
    nats: &async_nats::Client,
    symbols: &[String],
) -> Result<()> {
    let cycle_started_at = Instant::now();
    let symbol_batches = split_symbols_into_workers(symbols, SNAPSHOT_WORKER_COUNT);
    let snapshot_started_at = Instant::now();
    let responses = try_join_all(symbol_batches.iter().enumerate().map(|(index, batch)| {
        fetch_full_tick_snapshot_batch(client.clone(), index + 1, batch.clone())
    }))
    .await?;
    let snapshot_elapsed = snapshot_started_at.elapsed();
    let publish_started_at = Instant::now();
    let snapshot_count = responses.iter().map(|snapshots| snapshots.len()).sum::<usize>();
    let mut published_count = 0usize;
    let batch_count = symbol_batches.len();

    info!(
        target: "rstock::whole_quote",
        symbol_count = symbols.len(),
        batch_count,
        nats_url = %state.args.nats_url,
        snapshot_ms = snapshot_elapsed.as_millis(),
        snapshot_count,
        "full tick snapshot fetched"
    );

    for snapshots in responses {
        for snapshot in snapshots {
            if let Some((subject, payload)) = build_snapshot_message(snapshot)? {
                publish_tick(&nats, &subject, payload).await?;
                published_count += 1;
            }
        }
    }

    let publish_elapsed = publish_started_at.elapsed();
    let cycle_elapsed = cycle_started_at.elapsed();
    info!(
        target: "rstock::whole_quote",
        symbol_count = symbols.len(),
        batch_count,
        snapshot_count,
        published_count,
        snapshot_ms = snapshot_elapsed.as_millis(),
        publish_ms = publish_elapsed.as_millis(),
        cycle_ms = cycle_elapsed.as_millis(),
        "full tick snapshot cycle completed"
    );

    Ok(())
}

async fn fetch_full_tick_snapshot_batch(
    client: QmtClient,
    worker_id: usize,
    symbols: Vec<String>,
) -> Result<Vec<FullTickSnapshot>> {
    let started_at = Instant::now();
    let response = client
        .data()
        .get_full_tick_snapshot(FullTickSnapshotRequest {
            symbols: symbols.clone(),
        })
        .await
        .with_context(|| format!("get full tick snapshot failed for worker {worker_id}"))?
        .into_inner();
    ensure_status_ok(response.status.as_ref(), "GetFullTickSnapshot")?;
    let elapsed = started_at.elapsed();
    let snapshot_count = response.snapshots.len();
    info!(
        target: "rstock::whole_quote",
        worker_id,
        symbol_count = symbols.len(),
        snapshot_count,
        snapshot_ms = elapsed.as_millis(),
        "full tick snapshot batch fetched"
    );
    Ok(response.snapshots)
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

fn build_snapshot_message(snapshot: FullTickSnapshot) -> Result<Option<(String, Vec<u8>)>> {
    let Some(tick) = snapshot.tick else {
        return Ok(None);
    };
    let (code, market) = split_symbol(&snapshot.symbol)
        .ok_or_else(|| anyhow!("unsupported symbol format: {}", snapshot.symbol))?;
    let subject = format!("stock.tick.{code}.{market}");
    let full_code = format!("{code}.{market}");
    let payload = json!({
        "code": full_code,
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

async fn discover_symbols_by_sector(client: &QmtClient, sector_name: &str) -> Result<Vec<String>> {
    let grpc_started_at = Instant::now();
    let response = client
        .data()
        .get_stock_list_in_sector(StockListInSectorRequest {
            sector_name: sector_name.to_string(),
        })
        .await
        .with_context(|| format!("get stock list in sector failed: {sector_name}"))?
        .into_inner();
    ensure_status_ok(response.status.as_ref(), "GetStockListInSector")?;
    let grpc_elapsed = grpc_started_at.elapsed();
    let extract_started_at = Instant::now();
    let symbols = response
        .sector
        .map(|sector| sector.symbols)
        .unwrap_or_default()
        .into_iter()
        .map(|symbol| symbol.trim().to_string())
        .filter(|symbol| !symbol.is_empty())
        .collect::<Vec<_>>();
    let extract_elapsed = extract_started_at.elapsed();
    if symbols.is_empty() {
        return Err(anyhow!("sector returned no symbols: {sector_name}"));
    }
    info!(
        target: "rstock::whole_quote",
        sector_name = %sector_name,
        symbol_count = symbols.len(),
        get_stock_list_in_sector_ms = grpc_elapsed.as_millis(),
        extract_symbols_ms = extract_elapsed.as_millis(),
        "resolved symbols by sector"
    );
    Ok(symbols)
}

fn split_symbols_into_workers(symbols: &[String], worker_count: usize) -> Vec<Vec<String>> {
    if symbols.is_empty() {
        return Vec::new();
    }
    let batch_size = symbols.len().div_ceil(worker_count.max(1));
    symbols
        .chunks(batch_size.max(1))
        .map(|chunk| chunk.to_vec())
        .collect()
}

fn normalize_sector_name(sector_name: Option<String>) -> String {
    sector_name
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| DEFAULT_SECTOR_NAME.to_string())
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
