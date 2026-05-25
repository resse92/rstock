use std::collections::HashMap;
use std::time::{Duration, Instant};

use flate2::read::ZlibDecoder;
use std::io::Read;
use tokio::sync::Mutex;

use crate::error::{Result, TdxError};
use crate::net::async_connection::AsyncTcpConnection;
use crate::net::packet::{ResponseHeader, RSP_HEADER_LEN};
use crate::net::utils;
use crate::protocol::constants::*;
use crate::protocol::parsers::*;
use crate::protocol::types::*;

/// 缓存条目
struct CacheEntry<T> {
    data: T,
    expires_at: Instant,
}

/// 异步 TDX 行情客户端
///
/// 使用 tokio 异步 I/O，支持并发请求。
/// API 与同步版本 TdxHqClient 相同，但所有方法都是 async。
pub struct AsyncTdxHqClient {
    conn: Mutex<Option<AsyncTcpConnection>>,
    connected: std::sync::atomic::AtomicBool,
    ip: Mutex<String>,
    port: Mutex<u16>,
    last_server: Mutex<Option<(String, u16)>>,
    count_cache: Mutex<HashMap<u8, CacheEntry<u16>>>,
    list_cache: Mutex<HashMap<u8, CacheEntry<Vec<SecurityInfo>>>>,
    cache_ttl: Duration,
}

impl AsyncTdxHqClient {
    pub fn new() -> Self {
        Self {
            conn: Mutex::new(None),
            connected: std::sync::atomic::AtomicBool::new(false),
            ip: Mutex::new(String::new()),
            port: Mutex::new(0),
            last_server: Mutex::new(None),
            count_cache: Mutex::new(HashMap::new()),
            list_cache: Mutex::new(HashMap::new()),
            cache_ttl: Duration::from_secs(30),
        }
    }

    /// 连接到 TDX 服务器
    pub async fn connect(&self, ip: &str, port: u16, timeout: Option<f64>) -> Result<bool> {
        self.connect_internal(ip, port, timeout).await
    }

    /// 连接到任意可用服务器
    pub async fn connect_to_any(&self, timeout: Option<f64>) -> Result<bool> {
        for &(_, ip, port) in DEFAULT_SERVERS {
            match self.connect_internal(ip, port, timeout).await {
                Ok(true) => return Ok(true),
                _ => continue,
            }
        }
        Err(TdxError::Connection(
            "All servers unreachable".into(),
        ))
    }

    async fn connect_internal(
        &self,
        ip: &str,
        port: u16,
        timeout: Option<f64>,
    ) -> Result<bool> {
        let timeout_secs = timeout.unwrap_or(CONNECT_TIMEOUT);
        let mut tcp = AsyncTcpConnection::connect(ip, port, timeout_secs).await?;

        // 3-step handshake
        for cmd in &[SETUP_CMD1, SETUP_CMD2, SETUP_CMD3] {
            tcp.send(cmd).await?;
            let head = tcp.recv(RSP_HEADER_LEN).await?;
            let header = ResponseHeader::parse(&head)?;
            let zip_size = header.zip_size as usize;
            let unzip_size = header.unzip_size as usize;
            let mut body = Vec::with_capacity(zip_size);
            while body.len() < zip_size {
                let chunk = tcp.recv(zip_size - body.len()).await?;
                body.extend_from_slice(&chunk);
            }
            if zip_size != unzip_size {
                let mut decoder = ZlibDecoder::new(&body[..]);
                let mut decompressed = Vec::new();
                decoder.read_to_end(&mut decompressed).map_err(|e| {
                    TdxError::ResponseParse(format!("handshake decompress failed: {}", e))
                })?;
            }
        }

        {
            let mut conn = self.conn.lock().await;
            *conn = Some(tcp);
        }
        *self.ip.lock().await = ip.to_string();
        *self.port.lock().await = port;
        self.connected
            .store(true, std::sync::atomic::Ordering::SeqCst);
        *self.last_server.lock().await = Some((ip.to_string(), port));

        self.count_cache.lock().await.clear();
        self.list_cache.lock().await.clear();

        Ok(true)
    }

    /// 断开连接
    pub async fn disconnect(&self) {
        let mut conn = self.conn.lock().await;
        if let Some(mut c) = conn.take() {
            c.close();
        }
        self.connected
            .store(false, std::sync::atomic::Ordering::SeqCst);
    }

    /// 是否已连接
    pub fn is_connected(&self) -> bool {
        self.connected
            .load(std::sync::atomic::Ordering::SeqCst)
    }

    /// 设置缓存 TTL
    pub fn set_cache_ttl(&mut self, ttl_secs: u64) {
        self.cache_ttl = Duration::from_secs(ttl_secs);
    }

    // ================================================================
    // Internal send/recv
    // ================================================================

    async fn send_and_recv(&self, packet: &[u8]) -> Result<Vec<u8>> {
        let mut conn = self.conn.lock().await;
        let tcp = conn.as_mut().ok_or(TdxError::Disconnected)?;

        tcp.send(packet).await?;

        let head_buf = tcp.recv(RSP_HEADER_LEN).await?;
        let header = ResponseHeader::parse(&head_buf)?;

        let zip_size = header.zip_size as usize;
        let mut body_buf = Vec::with_capacity(zip_size);
        while body_buf.len() < zip_size {
            let remaining = zip_size - body_buf.len();
            let chunk = tcp.recv(remaining).await?;
            body_buf.extend_from_slice(&chunk);
        }

        if body_buf.is_empty() {
            return Err(TdxError::Disconnected);
        }

        if header.zip_size != header.unzip_size {
            let mut decoder = ZlibDecoder::new(&body_buf[..]);
            let mut decompressed = Vec::new();
            decoder.read_to_end(&mut decompressed).map_err(|e| {
                TdxError::ResponseParse(format!("zlib decompress failed: {}", e))
            })?;
            Ok(decompressed)
        } else {
            Ok(body_buf)
        }
    }

    /// 为复权计算获取额外的历史 K 线上下文 (异步版)
    async fn fetch_context_bars_for_adjust(
        &self,
        category: u8,
        market: u8,
        code: &str,
        bars: &[SecurityBar],
        xdxr: &[XdXrInfo],
    ) -> Vec<SecurityBar> {
        if bars.is_empty() || xdxr.is_empty() {
            return Vec::new();
        }

        let earliest_event = xdxr
            .iter()
            .filter(|x| x.category == 1)
            .map(|x| x.year as u32 * 10000 + x.month as u32 * 100 + x.day as u32)
            .min();

        let Some(ee_date) = earliest_event else { return Vec::new() };

        let first_bar_date =
            bars[0].year as u32 * 10000 + bars[0].month as u32 * 100 + bars[0].day as u32;

        if first_bar_date <= ee_date {
            return Vec::new();
        }

        let max_per_page = MAX_KLINE_COUNT as u32;
        let mut context = Vec::new();
        let mut offset = max_per_page;

        for _page in 0..8 {
            let pkt = utils::build_security_bars_packet(category, market, code, offset, MAX_KLINE_COUNT, 0);
            let body = match self.send_and_recv(&pkt).await {
                Ok(b) => b,
                Err(_) => break,
            };
            let batch = match parse_security_bars(&body, category) {
                Ok(b) => b,
                Err(_) => break,
            };
            if batch.is_empty() {
                break;
            }

            let batch_first_date =
                batch[0].year as u32 * 10000 + batch[0].month as u32 * 100 + batch[0].day as u32;

            let len_before = context.len();
            context.splice(0..0, batch);

            if batch_first_date <= ee_date {
                break;
            }

            offset += max_per_page;
            if context.len() == len_before {
                break;
            }
        }

        context
    }

    // ================================================================
    // API Methods
    // ================================================================

    pub async fn get_security_bars(
        &self,
        category: u8,
        market: u8,
        code: &str,
        start: u32,
        count: u16,
        fq: u8,
    ) -> Result<Vec<SecurityBar>> {
        let packet = utils::build_security_bars_packet(category, market, code, start, count, fq);
        let body = self.send_and_recv(&packet).await?;
        let mut bars = parse_security_bars(&body, category)?;
        if fq != 0 {
            if let Ok(xdxr) = self.get_xdxr_info(market, code).await {
                use crate::protocol::adjuster::{adjust_security_bars, FqType};
                let fq_enum = if fq == 2 { FqType::Hfq } else { FqType::Qfq };
                let context = self.fetch_context_bars_for_adjust(category, market, code, &bars, &xdxr).await;
                adjust_security_bars(&mut bars, &context, &xdxr, fq_enum);
            }
        }
        Ok(bars)
    }

    /// 自动分页获取K线
    pub async fn get_security_bars_all(
        &self,
        category: u8,
        market: u8,
        code: &str,
        count: u16,
        fq: u8,
    ) -> Result<Vec<SecurityBar>> {
        let mut all_bars = Vec::new();
        let mut offset = 0u32;
        let mut remaining = count;

        while remaining > 0 {
            let batch = remaining.min(MAX_KLINE_COUNT);
            let bars = self
                .get_security_bars(category, market, code, offset, batch, fq)
                .await?;
            if bars.is_empty() {
                break;
            }
            let fetched = bars.len() as u16;
            all_bars.extend(bars);
            remaining = remaining.saturating_sub(fetched);
            offset += fetched as u32;
            if fetched < batch {
                break;
            }
        }

        Ok(all_bars)
    }

    /// 获取指数K线 — 指数不存在复权概念，fq 参数保留接口一致性但不生效
    pub async fn get_index_bars(
        &self,
        category: u8,
        market: u8,
        code: &str,
        start: u32,
        count: u16,
        fq: u8,
    ) -> Result<Vec<IndexBar>> {
        let _ = fq; // 指数不复权，强制 fq=0 发送
        let packet = utils::build_index_bars_packet(category, market, code, start, count, 0);
        let body = self.send_and_recv(&packet).await?;
        parse_index_bars(&body, category)
    }

    pub async fn get_security_quotes(
        &self,
        all_stock: &[(u8, &str)],
    ) -> Result<Vec<SecurityQuote>> {
        let stock_len = all_stock.len() as u16;
        let pkgdatalen = (stock_len as u32) * 7 + 12;

        let mut packet = Vec::with_capacity(26 + stock_len as usize * 7);
        packet.extend_from_slice(&0x010Cu16.to_le_bytes());
        packet.extend_from_slice(&0x02006320u32.to_le_bytes());
        packet.extend_from_slice(&(pkgdatalen as u16).to_le_bytes());
        packet.extend_from_slice(&(pkgdatalen as u16).to_le_bytes());
        packet.extend_from_slice(&CMD_SECURITY_QUOTES.to_le_bytes());
        packet.extend_from_slice(&0u32.to_le_bytes());
        packet.extend_from_slice(&0u16.to_le_bytes());
        packet.extend_from_slice(&stock_len.to_le_bytes());
        for &(market, code) in all_stock {
            packet.push(market);
            packet.extend_from_slice(&utils::code_bytes(code));
        }

        let body = self.send_and_recv(&packet).await?;
        parse_security_quotes(&body)
    }

    pub async fn get_security_list(
        &self,
        market: u8,
        start: u16,
    ) -> Result<Vec<SecurityInfo>> {
        if start == 0 {
            let cache = self.list_cache.lock().await;
            if let Some(entry) = cache.get(&market) {
                if Instant::now() < entry.expires_at {
                    return Ok(entry.data.clone());
                }
            }
            drop(cache);

            let mut packet = Vec::with_capacity(16);
            packet.extend_from_slice(&[
                0x0c, 0x01, 0x18, 0x64, 0x01, 0x01, 0x06, 0x00, 0x06, 0x00, 0x50, 0x04,
            ]);
            packet.extend_from_slice(&(market as u16).to_le_bytes());
            packet.extend_from_slice(&start.to_le_bytes());

            let body = self.send_and_recv(&packet).await?;
            let result = parse_security_list(&body)?;

            let mut cache = self.list_cache.lock().await;
            cache.insert(
                market,
                CacheEntry {
                    data: result.clone(),
                    expires_at: Instant::now() + self.cache_ttl,
                },
            );
            return Ok(result);
        }

        let mut packet = Vec::with_capacity(16);
        packet.extend_from_slice(&[
            0x0c, 0x01, 0x18, 0x64, 0x01, 0x01, 0x06, 0x00, 0x06, 0x00, 0x50, 0x04,
        ]);
        packet.extend_from_slice(&(market as u16).to_le_bytes());
        packet.extend_from_slice(&start.to_le_bytes());

        let body = self.send_and_recv(&packet).await?;
        parse_security_list(&body)
    }

    pub async fn get_security_count(&self, market: u8) -> Result<u16> {
        let cache = self.count_cache.lock().await;
        if let Some(entry) = cache.get(&market) {
            if Instant::now() < entry.expires_at {
                return Ok(entry.data);
            }
        }
        drop(cache);

        let mut packet = Vec::with_capacity(18);
        packet.extend_from_slice(&[
            0x0c, 0x0c, 0x18, 0x6c, 0x00, 0x01, 0x08, 0x00, 0x08, 0x00, 0x4e, 0x04,
        ]);
        packet.extend_from_slice(&(market as u16).to_le_bytes());
        packet.extend_from_slice(&[0x75, 0xc7, 0x33, 0x01]);

        let body = self.send_and_recv(&packet).await?;
        let count = parse_security_count(&body)?;

        let mut cache = self.count_cache.lock().await;
        cache.insert(
            market,
            CacheEntry {
                data: count,
                expires_at: Instant::now() + self.cache_ttl,
            },
        );
        Ok(count)
    }

    pub async fn get_minute_time_data(
        &self,
        market: u8,
        code: &str,
    ) -> Result<Vec<MinuteTimePrice>> {
        let code_buf = utils::code_bytes(code);
        let mut packet = Vec::with_capacity(24);
        packet.extend_from_slice(&[
            0x0c, 0x1b, 0x08, 0x00, 0x01, 0x01, 0x0e, 0x00, 0x0e, 0x00, 0x1d, 0x05,
        ]);
        packet.extend_from_slice(&(market as u16).to_le_bytes());
        packet.extend_from_slice(&code_buf);
        packet.extend_from_slice(&0u32.to_le_bytes());

        let body = self.send_and_recv(&packet).await?;
        parse_minute_time_data(&body, market, code)
    }

    pub async fn get_history_minute_time_data(
        &self,
        market: u8,
        code: &str,
        date: u32,
    ) -> Result<Vec<MinuteTimePrice>> {
        let code_buf = utils::code_bytes(code);
        let mut packet = Vec::with_capacity(23);
        packet.extend_from_slice(&[
            0x0c, 0x01, 0x30, 0x00, 0x01, 0x01, 0x0d, 0x00, 0x0d, 0x00, 0xb4, 0x0f,
        ]);
        packet.extend_from_slice(&date.to_le_bytes());
        packet.push(market);
        packet.extend_from_slice(&code_buf);

        let body = self.send_and_recv(&packet).await?;
        parse_history_minute_time_data(&body, market, code)
    }

    pub async fn get_transaction_data(
        &self,
        market: u8,
        code: &str,
        start: u16,
        count: u16,
    ) -> Result<Vec<TickData>> {
        let code_buf = utils::code_bytes(code);
        let mut packet = Vec::with_capacity(24);
        packet.extend_from_slice(&[
            0x0c, 0x17, 0x08, 0x01, 0x01, 0x01, 0x0e, 0x00, 0x0e, 0x00, 0xc5, 0x0f,
        ]);
        packet.extend_from_slice(&(market as u16).to_le_bytes());
        packet.extend_from_slice(&code_buf);
        packet.extend_from_slice(&start.to_le_bytes());
        packet.extend_from_slice(&count.to_le_bytes());

        let body = self.send_and_recv(&packet).await?;
        parse_transaction_data(&body)
    }

    pub async fn get_history_transaction_data(
        &self,
        market: u8,
        code: &str,
        start: u16,
        count: u16,
        date: u32,
    ) -> Result<Vec<TickData>> {
        let code_buf = utils::code_bytes(code);
        let mut packet = Vec::with_capacity(28);
        packet.extend_from_slice(&[
            0x0c, 0x01, 0x30, 0x01, 0x00, 0x01, 0x12, 0x00, 0x12, 0x00, 0xb5, 0x0f,
        ]);
        packet.extend_from_slice(&date.to_le_bytes());
        packet.extend_from_slice(&(market as u16).to_le_bytes());
        packet.extend_from_slice(&code_buf);
        packet.extend_from_slice(&start.to_le_bytes());
        packet.extend_from_slice(&count.to_le_bytes());

        let body = self.send_and_recv(&packet).await?;
        parse_history_transaction_data(&body)
    }

    pub async fn get_finance_info(&self, market: u8, code: &str) -> Result<FinanceInfo> {
        let code_buf = utils::code_bytes(code);
        let mut packet = Vec::with_capacity(21);
        packet.extend_from_slice(&[
            0x0c, 0x1f, 0x18, 0x76, 0x00, 0x01, 0x0b, 0x00, 0x0b, 0x00, 0x10, 0x00, 0x01, 0x00,
        ]);
        packet.push(market);
        packet.extend_from_slice(&code_buf);

        let body = self.send_and_recv(&packet).await?;
        parse_finance_info(&body, market, code)
    }

    pub async fn get_xdxr_info(&self, market: u8, code: &str) -> Result<Vec<XdXrInfo>> {
        let code_buf = utils::code_bytes(code);
        let mut packet = Vec::with_capacity(21);
        packet.extend_from_slice(&[
            0x0c, 0x1f, 0x18, 0x76, 0x00, 0x01, 0x0b, 0x00, 0x0b, 0x00, 0x0f, 0x00, 0x01, 0x00,
        ]);
        packet.push(market);
        packet.extend_from_slice(&code_buf);

        let body = self.send_and_recv(&packet).await?;
        parse_xdxr_info(&body)
    }
}
