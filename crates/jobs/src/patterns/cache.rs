use std::collections::HashMap;
use std::path::Path;

use anyhow::{Context, Result};
use chrono::{Duration, NaiveDate, Utc};
use duckdb::{params, Connection};

use crate::patterns::model::{Bar, BarSeries, PatternCacheConfig};

pub struct DuckDbPatternCache {
    config: PatternCacheConfig,
}

impl DuckDbPatternCache {
    pub fn new(config: PatternCacheConfig) -> Result<Self> {
        let cache = Self { config };
        cache.initialize()?;
        Ok(cache)
    }

    pub fn latest_dates(&self, symbols: &[String]) -> Result<HashMap<String, NaiveDate>> {
        let conn = self.open()?;
        let mut stmt = conn.prepare(
            "SELECT symbol, MAX(trade_date) AS latest_date
             FROM daily_bars
             WHERE symbol = ?
             GROUP BY symbol",
        )?;

        let mut latest = HashMap::new();
        for symbol in symbols {
            let mut rows = stmt.query(params![symbol])?;
            while let Some(row) = rows.next()? {
                let date_str: String = row.get(1)?;
                let date = NaiveDate::parse_from_str(&date_str, "%Y-%m-%d")?;
                latest.insert(symbol.clone(), date);
            }
        }
        Ok(latest)
    }

    pub fn upsert_daily_bars(&self, bars: &[Bar]) -> Result<()> {
        if bars.is_empty() {
            return Ok(());
        }

        let mut conn = self.open()?;
        let tx = conn.transaction()?;
        {
            let mut stmt = tx.prepare(
                "INSERT INTO daily_bars (
                    symbol, exchange, trade_date, open, high, low, close, volume, amount, source, updated_at
                 ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                 ON CONFLICT(symbol, trade_date) DO UPDATE SET
                    exchange = excluded.exchange,
                    open = excluded.open,
                    high = excluded.high,
                    low = excluded.low,
                    close = excluded.close,
                    volume = excluded.volume,
                    amount = excluded.amount,
                    source = excluded.source,
                    updated_at = CURRENT_TIMESTAMP",
            )?;

            for bar in bars {
                stmt.execute(params![
                    &bar.symbol,
                    &bar.exchange,
                    bar.time.format("%Y-%m-%d").to_string(),
                    bar.open,
                    bar.high,
                    bar.low,
                    bar.close,
                    bar.volume,
                    bar.amount,
                    bar.source.clone().unwrap_or_else(|| "unknown".to_string()),
                ])?;
            }
        }
        tx.commit()?;
        self.cleanup()?;
        Ok(())
    }

    pub fn load_series(
        &self,
        symbols: &[String],
        start_date: NaiveDate,
        end_date: NaiveDate,
    ) -> Result<Vec<BarSeries>> {
        let conn = self.open()?;
        let mut stmt = conn.prepare(
            "SELECT symbol, exchange, trade_date, open, high, low, close, volume, amount, source
             FROM daily_bars
             WHERE symbol = ? AND trade_date >= ? AND trade_date <= ?
             ORDER BY trade_date ASC",
        )?;

        let start = start_date.format("%Y-%m-%d").to_string();
        let end = end_date.format("%Y-%m-%d").to_string();
        let mut grouped: HashMap<String, (String, Vec<Bar>)> = HashMap::new();

        for symbol in symbols {
            let mut rows = stmt.query(params![symbol, &start, &end])?;
            while let Some(row) = rows.next()? {
                let symbol: String = row.get(0)?;
                let exchange: String = row.get(1)?;
                let date_str: String = row.get(2)?;
                let time = NaiveDate::parse_from_str(&date_str, "%Y-%m-%d")?;
                let bar = Bar {
                    symbol: symbol.clone(),
                    exchange: exchange.clone(),
                    time,
                    open: row.get(3)?,
                    high: row.get(4)?,
                    low: row.get(5)?,
                    close: row.get(6)?,
                    volume: row.get(7)?,
                    amount: row.get(8)?,
                    source: Some(row.get::<_, String>(9)?),
                };
                grouped
                    .entry(symbol)
                    .or_insert_with(|| (exchange, Vec::new()))
                    .1
                    .push(bar);
            }
        }

        Ok(grouped
            .into_iter()
            .map(|(symbol, (exchange, bars))| BarSeries::new(symbol, exchange, bars))
            .collect())
    }

    pub fn cleanup(&self) -> Result<()> {
        let conn = self.open()?;
        let cutoff = (Utc::now().date_naive() - Duration::days(self.config.retention_days))
            .format("%Y-%m-%d")
            .to_string();

        conn.execute(
            "DELETE FROM daily_bars WHERE trade_date < ?",
            params![cutoff],
        )?;

        let count: i64 = conn.query_row("SELECT COUNT(*) FROM daily_bars", [], |row| row.get(0))?;
        let max_bars = self.config.max_bars as i64;
        if count > max_bars {
            let overflow = count - max_bars;
            conn.execute(
                "DELETE FROM daily_bars
                 WHERE (symbol, trade_date) IN (
                    SELECT symbol, trade_date
                    FROM daily_bars
                    ORDER BY trade_date ASC, symbol ASC
                    LIMIT ?
                 )",
                params![overflow],
            )?;
        }

        conn.execute_batch("CHECKPOINT;")?;
        Ok(())
    }

    fn initialize(&self) -> Result<()> {
        if let Some(parent) = self.config.db_path.parent() {
            std::fs::create_dir_all(parent).with_context(|| {
                format!("failed to create cache directory {}", parent.display())
            })?;
        }
        let conn = self.open()?;
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS daily_bars (
                symbol TEXT NOT NULL,
                exchange TEXT NOT NULL,
                trade_date TEXT NOT NULL,
                open DOUBLE NOT NULL,
                high DOUBLE NOT NULL,
                low DOUBLE NOT NULL,
                close DOUBLE NOT NULL,
                volume DOUBLE NOT NULL,
                amount DOUBLE,
                source TEXT,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY(symbol, trade_date)
            );",
        )?;
        Ok(())
    }

    fn open(&self) -> Result<Connection> {
        Connection::open(&self.config.db_path)
            .with_context(|| format!("failed to open duckdb {}", self.config.db_path.display()))
    }
}

#[allow(dead_code)]
fn _assert_path_is_local(path: &Path) -> bool {
    !path.as_os_str().is_empty()
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use chrono::NaiveDate;

    use crate::patterns::model::{Bar, PatternCacheConfig};

    use super::DuckDbPatternCache;

    #[test]
    fn cleanup_keeps_cache_bounded() {
        let db_path =
            std::env::temp_dir().join(format!("patterns-cache-{}.duckdb", std::process::id()));
        let _ = std::fs::remove_file(&db_path);
        let cache = DuckDbPatternCache::new(PatternCacheConfig {
            db_path: PathBuf::from(&db_path),
            retention_days: 5000,
            max_bars: 3,
        })
        .unwrap();

        let mut bars = Vec::new();
        for idx in 0..5 {
            bars.push(Bar {
                symbol: format!("00000{idx}.SZ"),
                exchange: "SZ".to_string(),
                time: NaiveDate::from_ymd_opt(2025, 1, 1 + idx as u32).unwrap(),
                open: 10.0,
                high: 10.2,
                low: 9.8,
                close: 10.1,
                volume: 1000.0,
                amount: None,
                source: Some("test".to_string()),
            });
        }

        cache.upsert_daily_bars(&bars).unwrap();
        let latest = cache
            .latest_dates(
                &bars
                    .iter()
                    .map(|bar| bar.symbol.clone())
                    .collect::<Vec<_>>(),
            )
            .unwrap();
        assert!(latest.len() <= 3);
        let _ = std::fs::remove_file(&db_path);
    }
}
