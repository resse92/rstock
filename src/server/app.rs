use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use chrono::{DateTime, Duration, NaiveDate, Utc};
use jobs::patterns::{PatternScanFailure, PatternScanReport};
use tokio::sync::Mutex;

use super::feishu::FeishuNotifier;
use super::whole_quote::WholeQuoteSubscription;

use super::config::ServerConfig;

pub const MARKET_SCAN_JOB_TTL_HOURS: i64 = 24;
pub const MARKET_SCAN_JOB_MAX_COUNT: usize = 200;

#[derive(Debug, Clone)]
pub struct MarketScanJob {
    pub job_id: String,
    pub pattern_id: String,
    pub trade_date: NaiveDate,
    pub history_days: i64,
    pub refresh_remote: bool,
    pub status: MarketScanJobStatus,
    pub requested_symbols: usize,
    pub resolved_symbols: usize,
    pub completed_symbols: usize,
    pub series_count: usize,
    pub skipped_short_series: usize,
    pub signal_count: usize,
    pub failed_symbols: Vec<PatternScanFailure>,
    pub result: Option<PatternScanReport>,
    pub error: Option<String>,
    pub created_at: DateTime<Utc>,
    pub started_at: Option<DateTime<Utc>>,
    pub finished_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MarketScanJobStatus {
    Queued,
    Running,
    Succeeded,
    Failed,
}

#[derive(Clone)]
pub struct AppState {
    pub args: Arc<ServerConfig>,
    pub sync_lock: Arc<Mutex<()>>,
    pub whole_quote: Arc<Mutex<WholeQuoteSubscription>>,
    pub market_scan_jobs: Arc<Mutex<HashMap<String, MarketScanJob>>>,
    pub next_market_scan_job_id: Arc<AtomicU64>,
    pub feishu: Arc<FeishuNotifier>,
}

impl AppState {
    pub fn new(args: ServerConfig) -> Self {
        let feishu_bot_token = args.feishu_bot_token.clone();
        Self {
            args: Arc::new(args),
            sync_lock: Arc::new(Mutex::new(())),
            whole_quote: Arc::new(Mutex::new(WholeQuoteSubscription::default())),
            market_scan_jobs: Arc::new(Mutex::new(HashMap::new())),
            next_market_scan_job_id: Arc::new(AtomicU64::new(1)),
            feishu: Arc::new(FeishuNotifier::new(feishu_bot_token)),
        }
    }

    pub fn next_market_scan_job_id(&self) -> String {
        let id = self.next_market_scan_job_id.fetch_add(1, Ordering::Relaxed);
        format!("market-scan-{id}")
    }
}

pub fn cleanup_market_scan_jobs(jobs: &mut HashMap<String, MarketScanJob>, now: DateTime<Utc>) {
    let expire_before = now - Duration::hours(MARKET_SCAN_JOB_TTL_HOURS);
    jobs.retain(|_, job| {
        if !matches!(
            job.status,
            MarketScanJobStatus::Succeeded | MarketScanJobStatus::Failed
        ) {
            return true;
        }

        let finished_at = job.finished_at.unwrap_or(job.created_at);
        finished_at >= expire_before
    });

    if jobs.len() <= MARKET_SCAN_JOB_MAX_COUNT {
        return;
    }

    let mut removable = jobs
        .iter()
        .filter_map(|(job_id, job)| {
            if matches!(
                job.status,
                MarketScanJobStatus::Succeeded | MarketScanJobStatus::Failed
            ) {
                Some((
                    job_id.clone(),
                    job.finished_at.unwrap_or(job.created_at),
                    job.created_at,
                ))
            } else {
                None
            }
        })
        .collect::<Vec<_>>();
    removable.sort_by_key(|(_, finished_at, created_at)| (*finished_at, *created_at));

    let overflow = jobs.len().saturating_sub(MARKET_SCAN_JOB_MAX_COUNT);
    for (job_id, _, _) in removable.into_iter().take(overflow) {
        jobs.remove(&job_id);
    }
}
