use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use flate2::read::ZlibDecoder;
use std::io::Read;

use crate::error::{Result, TdxError};
use crate::net::connection::TcpConnection;
use crate::net::packet::{ResponseHeader, RSP_HEADER_LEN};
use crate::net::pool::{ConnectionPool, PoolConfig, PoolStats};
use crate::net::utils;
use crate::protocol::constants::*;
use crate::protocol::parsers::*;
use crate::protocol::types::*;

/// 缓存条目
struct CacheEntry<T> {
    data: T,
    expires_at: Instant,
}

/// TDX 行情客户端
pub struct TdxHqClient {
    pool: Mutex<Arc<ConnectionPool>>,
    connected: Arc<AtomicBool>,
    auto_retry: AtomicBool,
    heartbeat_stop: Mutex<Option<Arc<AtomicBool>>>,
    heartbeat_handle: Mutex<Option<std::thread::JoinHandle<()>>>,
    last_server: Mutex<Option<(String, u16)>>,
    count_cache: Mutex<HashMap<u8, CacheEntry<u16>>>,
    list_cache: Mutex<HashMap<u8, CacheEntry<Vec<SecurityInfo>>>>,
    cache_ttl: Mutex<Duration>,
    connect_timeout: Mutex<f64>,
    /// 用户自定义优先服务器列表 (为空时使用 PRIMARY_SERVERS)
    server_list: Mutex<Vec<(String, String, u16)>>,
}

impl TdxHqClient {
    pub fn new() -> Self {
        let config = PoolConfig::default();
        let default_server = (
            PRIMARY_SERVERS[0].1.to_string(),
            PRIMARY_SERVERS[0].2,
        );
        Self {
            pool: Mutex::new(Arc::new(ConnectionPool::new_single(default_server, config))),
            connected: Arc::new(AtomicBool::new(false)),
            auto_retry: AtomicBool::new(true),
            heartbeat_stop: Mutex::new(None),
            heartbeat_handle: Mutex::new(None),
            last_server: Mutex::new(None),
            count_cache: Mutex::new(HashMap::new()),
            list_cache: Mutex::new(HashMap::new()),
            cache_ttl: Mutex::new(Duration::from_secs(30)),
            connect_timeout: Mutex::new(CONNECT_TIMEOUT),
            server_list: Mutex::new(Vec::new()),
        }
    }

    /// 连接到 TDX 服务器
    pub fn connect(&self, ip: &str, port: u16, timeout: Option<f64>) -> Result<bool> {
        self.connect_internal(ip, port, timeout, true)
    }

    /// 连接到任意可用服务器
    ///
    /// 遍历顺序: 用户自定义列表 → PRIMARY_SERVERS → ALL_KNOWN_SERVERS
    pub fn connect_to_any(&self, timeout: Option<f64>) -> Result<bool> {
        // 1) 用户自定义列表
        {
            let list = self.server_list.lock().unwrap();
            for (_, ip, port) in list.iter() {
                match self.connect_internal(ip, *port, timeout, false) {
                    Ok(true) => return Ok(true),
                    _ => continue,
                }
            }
        }

        // 2) PRIMARY_SERVERS (10台)
        for &(_, ip, port) in PRIMARY_SERVERS {
            match self.connect_internal(ip, port, timeout, false) {
                Ok(true) => return Ok(true),
                _ => continue,
            }
        }

        // 3) ALL_KNOWN_SERVERS (101台, 兜底)
        for &(_, ip, port) in ALL_KNOWN_SERVERS {
            match self.connect_internal(ip, port, timeout, false) {
                Ok(true) => return Ok(true),
                _ => continue,
            }
        }

        Err(TdxError::Connection(
            "All servers unreachable".into(),
        ))
    }

    // ================================================================
    // 服务器列表管理
    // ================================================================

    /// 设置自定义优先服务器列表 (替换默认 PRIMARY_SERVERS)
    pub fn set_servers(&self, servers: &[(&str, &str, u16)]) {
        let mut list = self.server_list.lock().unwrap();
        list.clear();
        for &(name, ip, port) in servers {
            list.push((name.to_string(), ip.to_string(), port));
        }
    }

    /// 在优先列表头部添加一台服务器
    pub fn add_server(&self, name: &str, ip: &str, port: u16) {
        let mut list = self.server_list.lock().unwrap();
        list.insert(0, (name.to_string(), ip.to_string(), port));
    }

    /// 按响应时间重排优先服务器顺序 (最快在前)
    ///
    /// `sorted`: 由 probe_servers() 返回的排序结果, 取前N台
    pub fn reorder_servers(&self, sorted: &[(&str, &str, u16)]) {
        let mut list = self.server_list.lock().unwrap();
        list.clear();
        for &(name, ip, port) in sorted {
            list.push((name.to_string(), ip.to_string(), port));
        }
    }

    /// 探测所有已知服务器, 返回按 API 响应时间升序排列的结果
    ///
    /// 返回: Vec<(名称, IP, 端口, TCP延迟ms, 握手延迟ms, API延迟ms)>
    /// 不修改当前优先列表。用户根据结果自行调用 reorder_servers()
    pub fn probe_servers(
        &self,
        timeout_secs: f64,
    ) -> Vec<(&'static str, &'static str, u16, f64, f64, f64)> {
        let mut results = Vec::new();
        let _timeout = Some(timeout_secs);

        for &(name, ip, port) in ALL_KNOWN_SERVERS {
            let tcp_start = Instant::now();
            let result = (|| -> std::result::Result<(f64, f64), TdxError> {
                let mut tcp = TcpConnection::connect(ip, port, timeout_secs)?;
                let tcp_ms = tcp_start.elapsed().as_secs_f64() * 1000.0;

                let hs_start = Instant::now();
                utils::perform_handshake(&mut tcp)?;
                let hs_ms = hs_start.elapsed().as_secs_f64() * 1000.0;

                // 简单 API (get_security_count)
                // 验证服务器可用 (发送 API 请求)
                let mut pkt = Vec::with_capacity(18);
                pkt.extend_from_slice(&[
                    0x0c, 0x0c, 0x18, 0x6c, 0x00, 0x01, 0x08, 0x00, 0x08, 0x00, 0x4e, 0x04,
                ]);
                pkt.extend_from_slice(&(1u16.to_le_bytes())); // market=SH
                pkt.extend_from_slice(&[0x75, 0xc7, 0x33, 0x01]);
                tcp.send(&pkt)?;
                let head = tcp.recv(RSP_HEADER_LEN)?;
                let h = ResponseHeader::parse(&head)?;
                let zs = h.zip_size as usize;
                let mut body = Vec::with_capacity(zs);
                while body.len() < zs {
                    body.extend_from_slice(&tcp.recv(zs - body.len())?);
                }
                Ok((tcp_ms, hs_ms))
            })();

            match result {
                Ok((tcp_ms, hs_ms)) => {
                    let api_ms = {
                        let mut conn = TcpConnection::connect(ip, port, timeout_secs).unwrap();
                        utils::perform_handshake(&mut conn).unwrap();
                        let start = Instant::now();
                        let mut pkt = Vec::with_capacity(18);
                        pkt.extend_from_slice(&[
                            0x0c, 0x0c, 0x18, 0x6c, 0x00, 0x01, 0x08, 0x00, 0x08, 0x00, 0x4e, 0x04,
                        ]);
                        pkt.extend_from_slice(&(1u16.to_le_bytes()));
                        pkt.extend_from_slice(&[0x75, 0xc7, 0x33, 0x01]);
                        conn.send(&pkt).unwrap();
                        let head = conn.recv(RSP_HEADER_LEN).unwrap();
                        let h = ResponseHeader::parse(&head).unwrap();
                        let zs = h.zip_size as usize;
                        let mut body = Vec::with_capacity(zs);
                        while body.len() < zs {
                            body.extend_from_slice(&conn.recv(zs - body.len()).unwrap());
                        }
                        start.elapsed().as_secs_f64() * 1000.0
                    };
                    results.push((name, ip, port, tcp_ms, hs_ms, api_ms));
                }
                Err(_) => continue,
            }
        }

        results.sort_by(|a, b| a.5.partial_cmp(&b.5).unwrap());
        results
    }

    fn connect_internal(
        &self,
        ip: &str,
        port: u16,
        timeout: Option<f64>,
        start_heartbeat: bool,
    ) -> Result<bool> {
        let timeout_secs = timeout.unwrap_or(*self.connect_timeout.lock().unwrap());

        // 建立连接并执行握手
        let mut tcp = TcpConnection::connect(ip, port, timeout_secs)?;
        utils::perform_handshake(&mut tcp)?;
        // 握手成功，将连接放入池中
        let server = (ip.to_string(), port);
        let mut config = PoolConfig {
            max_size: DEFAULT_POOL_SIZE,
            connect_timeout: timeout_secs,
            handshake_fn: None,
        };
        // 设置握手回调，让新连接也能自动握手
        config.handshake_fn = Some(Box::new(|conn: &mut TcpConnection| -> Result<()> {
            utils::perform_handshake(conn)
        }));
        let pool = Arc::new(ConnectionPool::new_single(server.clone(), config));
        pool.push(tcp, server.clone());

        *self.pool.lock().unwrap() = pool;
        self.connected.store(true, Ordering::SeqCst);
        *self.last_server.lock().unwrap() = Some(server);

        // 清除缓存
        self.count_cache.lock().unwrap().clear();
        self.list_cache.lock().unwrap().clear();

        if start_heartbeat {
            self.start_heartbeat();
        }
        Ok(true)
    }

    /// 断开连接
    pub fn disconnect(&self) {
        self.stop_heartbeat();
        self.pool.lock().unwrap().close_all();
        self.connected.store(false, Ordering::SeqCst);
    }

    /// 是否已连接
    pub fn is_connected(&self) -> bool {
        self.connected.load(Ordering::SeqCst)
    }

    /// 设置是否自动重试
    pub fn set_auto_retry(&self, enabled: bool) {
        self.auto_retry.store(enabled, Ordering::SeqCst);
    }

    /// 设置缓存 TTL (秒)
    pub fn set_cache_ttl(&self, ttl_secs: u64) {
        *self.cache_ttl.lock().unwrap() = Duration::from_secs(ttl_secs);
    }

    /// 设置连接超时 (秒)
    pub fn set_connect_timeout(&self, timeout: f64) {
        *self.connect_timeout.lock().unwrap() = timeout;
    }

    /// 获取连接池状态
    pub fn pool_stats(&self) -> PoolStats {
        self.pool.lock().unwrap().stats()
    }

    // ================================================================
    // Heartbeat
    // ================================================================

    fn start_heartbeat(&self) {
        self.stop_heartbeat();
        let stop = Arc::new(AtomicBool::new(false));
        let pool = Arc::clone(&self.pool.lock().unwrap());
        let connected = Arc::clone(&self.connected);
        let stop_clone = stop.clone();
        let server = self.last_server.lock().unwrap().clone()
            .unwrap_or_else(|| (PRIMARY_SERVERS[0].1.to_string(), PRIMARY_SERVERS[0].2));
        let interval = Duration::from_secs_f64(DEFAULT_HEARTBEAT_INTERVAL);

        let handle = std::thread::spawn(move || {
            while !stop_clone.load(Ordering::Relaxed) {
                std::thread::sleep(interval);
                if stop_clone.load(Ordering::Relaxed) {
                    break;
                }

                // 尝试从池中借出连接做心跳
                if let Ok(Some(mut guard)) = pool.try_borrow(&server) {
                    let alive = (|| -> bool {
                        let conn = guard.conn();
                        let mut packet = Vec::with_capacity(18);
                        packet.extend_from_slice(&[
                            0x0c, 0x0c, 0x18, 0x6c, 0x00, 0x01, 0x08, 0x00, 0x08, 0x00, 0x4e,
                            0x04,
                        ]);
                        packet.extend_from_slice(&0u16.to_le_bytes());
                        packet.extend_from_slice(&[0x75, 0xc7, 0x33, 0x01]);

                        if conn.send(&packet).is_err() {
                            return false;
                        }
                        let head = match conn.recv(RSP_HEADER_LEN) {
                            Ok(h) => h,
                            Err(_) => return false,
                        };
                        let header = match ResponseHeader::parse(&head) {
                            Ok(h) => h,
                            Err(_) => return false,
                        };
                        let zip_size = header.zip_size as usize;
                        let mut body = Vec::with_capacity(zip_size);
                        while body.len() < zip_size {
                            match conn.recv(zip_size - body.len()) {
                                Ok(chunk) => body.extend_from_slice(&chunk),
                                Err(_) => return false,
                            }
                        }
                        true
                    })();

                    if !alive {
                        connected.store(false, Ordering::SeqCst);
                    }
                }
            }
        });

        *self.heartbeat_stop.lock().unwrap() = Some(stop);
        *self.heartbeat_handle.lock().unwrap() = Some(handle);
        self.connected.store(true, Ordering::SeqCst);
    }

    fn stop_heartbeat(&self) {
        if let Some(stop) = self.heartbeat_stop.lock().unwrap().take() {
            stop.store(true, Ordering::SeqCst);
        }
        if let Some(h) = self.heartbeat_handle.lock().unwrap().take() {
            let _ = h.join();
        }
    }

    // ================================================================
    // Internal send/recv with retry
    // ================================================================

    /// 发送请求并接收响应 (核心方法，带重试)
    fn send_and_recv(&self, packet: &[u8]) -> Result<Vec<u8>> {
        // 第一次尝试
        match self.try_send_and_recv(packet) {
            Ok(body) => return Ok(body),
            Err(e) if !self.auto_retry.load(Ordering::SeqCst) => return Err(e),
            Err(_) => {}
        }

        // 重试
        for &interval in RETRY_INTERVALS {
            std::thread::sleep(Duration::from_secs_f64(interval));

            // 尝试重连
            self.reconnect_if_needed();

            // 重试请求
            match self.try_send_and_recv(packet) {
                Ok(body) => return Ok(body),
                Err(_) => continue,
            }
        }

        Err(TdxError::RetryExhausted(RETRY_INTERVALS.len() + 1))
    }

    /// 从连接池借出连接并执行请求
    fn try_send_and_recv(&self, packet: &[u8]) -> Result<Vec<u8>> {
        let server = self.last_server.lock().unwrap().clone()
            .unwrap_or_else(|| (PRIMARY_SERVERS[0].1.to_string(), PRIMARY_SERVERS[0].2));
        let pool = self.pool.lock().unwrap();
        let mut guard = pool.borrow(&server).map_err(|e| {
            TdxError::Connection(format!("failed to borrow connection: {}", e))
        })?;

        let conn = guard.conn();

        // Send
        conn.send(packet)?;

        // Read response header
        let head_buf = conn.recv(RSP_HEADER_LEN)?;
        let header = ResponseHeader::parse(&head_buf)?;

        // Read body
        let zip_size = header.zip_size as usize;
        let mut body_buf = Vec::with_capacity(zip_size);
        while body_buf.len() < zip_size {
            let remaining = zip_size - body_buf.len();
            let chunk = conn.recv(remaining)?;
            body_buf.extend_from_slice(&chunk);
        }

        if body_buf.is_empty() {
            return Err(TdxError::Disconnected);
        }

        // Decompress if needed
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

    /// 尝试重连
    fn reconnect_if_needed(&self) {
        if self.connected.load(Ordering::SeqCst) {
            return;
        }

        let last = self.last_server.lock().unwrap().clone();
        if let Some((ref ip, port)) = last {
            if self.connect_internal(ip, port, Some(CONNECT_TIMEOUT), false).is_ok() {
                return;
            }
        }

        // 用户自定义 → PRIMARY → ALL_KNOWN
        {
            let list = self.server_list.lock().unwrap();
            for (_, ip, port) in list.iter() {
                if self.connect_internal(ip, *port, Some(CONNECT_TIMEOUT), false).is_ok() {
                    return;
                }
            }
        }
        for &(_, ip, port) in PRIMARY_SERVERS {
            if self.connect_internal(ip, port, Some(CONNECT_TIMEOUT), false).is_ok() {
                return;
            }
        }
        for &(_, ip, port) in ALL_KNOWN_SERVERS {
            if self.connect_internal(ip, port, Some(CONNECT_TIMEOUT), false).is_ok() {
                return;
            }
        }
    }

    // ================================================================
    // API Methods
    // ================================================================

    /// 获取K线数据
    ///
    /// `fq`: 复权类型, 0=未复权 1=前复权(默认) 2=后复权
    /// (v0.4.1: fq>0 时自动获取除权信息并客户端侧计算复权)
    pub fn get_security_bars(
        &self,
        category: u8,
        market: u8,
        code: &str,
        start: u32,
        count: u16,
        fq: u8,
    ) -> Result<Vec<SecurityBar>> {
        let packet = utils::build_security_bars_packet(category, market, code, start, count, fq);
        let body = self.send_and_recv(&packet)?;
        let mut bars = parse_security_bars(&body, category)?;

        // 客户端侧复权计算 (v0.4.2)
        if fq != 0 {
            if let Ok(xdxr) = self.get_xdxr_info(market, code) {
                use crate::protocol::adjuster::{adjust_security_bars, FqType};
                let fq_enum = match fq {
                    2 => FqType::Hfq,
                    _ => FqType::Qfq,
                };
                let context = self.fetch_context_bars_for_adjust(category, market, code, &bars, &xdxr);
                adjust_security_bars(&mut bars, &context, &xdxr, fq_enum);
            }
        }

        Ok(bars)
    }

    /// 为复权计算获取额外的历史 K 线上下文
    ///
    /// 当除权除息事件早于请求的 K 线数据范围时,
    /// 需要额外的历史数据来计算这些早期事件的复权因子。
    /// 为复权计算获取历史 K 线上下文
    fn fetch_context_bars_for_adjust(
        &self,
        category: u8,
        market: u8,
        code: &str,
        bars: &[SecurityBar],
        xdxr: &[XdXrInfo],
    ) -> Vec<SecurityBar> {
        utils::fetch_context_bars_for_adjust(
            |pkt| self.send_and_recv(pkt),
            category, market, code, bars, xdxr,
        )
    }

    /// 获取K线数据 (自动分页)
    ///
    /// 当 count > MAX_KLINE_COUNT 时自动分页请求并合并结果
    pub fn get_security_bars_all(
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
            let bars = self.get_security_bars(category, market, code, offset, batch, fq)?;
            if bars.is_empty() {
                break;
            }
            let fetched = bars.len() as u16;
            // TDX 返回升序(oldest→newest)，翻页是更早数据，prepend 保持整体升序
            all_bars.splice(0..0, bars);
            remaining = remaining.saturating_sub(fetched);
            offset += fetched as u32;

            if fetched < batch {
                break; // 没有更多数据
            }
        }

        Ok(all_bars)
    }

    /// 获取指数K线
    ///
    /// `fq`: 复权类型, 0=未复权 1=前复权(默认) 2=后复权
    /// 获取指数K线 — 指数不存在复权概念，fq 参数保留接口一致性但不生效
    pub fn get_index_bars(
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
        let body = self.send_and_recv(&packet)?;
        parse_index_bars(&body, category)
    }

    /// 获取指数K线 (自动分页)
    pub fn get_index_bars_all(
        &self,
        category: u8,
        market: u8,
        code: &str,
        count: u16,
        fq: u8,
    ) -> Result<Vec<IndexBar>> {
        let mut all_bars = Vec::new();
        let mut offset = 0u32;
        let mut remaining = count;

        while remaining > 0 {
            let batch = remaining.min(MAX_KLINE_COUNT);
            let bars = self.get_index_bars(category, market, code, offset, batch, fq)?;
            if bars.is_empty() {
                break;
            }
            let fetched = bars.len() as u16;
            // TDX 返回升序(oldest→newest)，翻页是更早数据，prepend 保持整体升序
            all_bars.splice(0..0, bars);
            remaining = remaining.saturating_sub(fetched);
            offset += fetched as u32;

            if fetched < batch {
                break;
            }
        }

        Ok(all_bars)
    }

    /// 获取实时行情
    pub fn get_security_quotes(
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

        let body = self.send_and_recv(&packet)?;
        parse_security_quotes(&body)
    }

    /// 获取证券列表 (带缓存)
    pub fn get_security_list(&self, market: u8, start: u16) -> Result<Vec<SecurityInfo>> {
        if start == 0 {
            let cache = self.list_cache.lock().unwrap();
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

            let body = self.send_and_recv(&packet)?;
            let result = parse_security_list(&body)?;

            let mut cache = self.list_cache.lock().unwrap();
            let ttl = *self.cache_ttl.lock().unwrap();
            cache.insert(
                market,
                CacheEntry {
                    data: result.clone(),
                    expires_at: Instant::now() + ttl,
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

        let body = self.send_and_recv(&packet)?;
        parse_security_list(&body)
    }

    /// 获取证券数量 (带缓存)
    pub fn get_security_count(&self, market: u8) -> Result<u16> {
        let cache = self.count_cache.lock().unwrap();
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

        let body = self.send_and_recv(&packet)?;
        let count = parse_security_count(&body)?;

        let mut cache = self.count_cache.lock().unwrap();
        let ttl = *self.cache_ttl.lock().unwrap();
        cache.insert(
            market,
            CacheEntry {
                data: count,
                expires_at: Instant::now() + ttl,
            },
        );
        Ok(count)
    }

    /// 获取分时数据
    pub fn get_minute_time_data(
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

        let body = self.send_and_recv(&packet)?;
        parse_minute_time_data(&body, market, code)
    }

    /// 获取历史分时数据
    pub fn get_history_minute_time_data(
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

        let body = self.send_and_recv(&packet)?;
        parse_history_minute_time_data(&body, market, code)
    }

    /// 获取逐笔成交
    pub fn get_transaction_data(
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

        let body = self.send_and_recv(&packet)?;
        parse_transaction_data(&body)
    }

    /// 获取历史逐笔成交
    pub fn get_history_transaction_data(
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

        let body = self.send_and_recv(&packet)?;
        parse_history_transaction_data(&body)
    }

    /// 获取财务信息
    pub fn get_finance_info(&self, market: u8, code: &str) -> Result<FinanceInfo> {
        let code_buf = utils::code_bytes(code);
        let mut packet = Vec::with_capacity(21);
        packet.extend_from_slice(&[
            0x0c, 0x1f, 0x18, 0x76, 0x00, 0x01, 0x0b, 0x00, 0x0b, 0x00, 0x10, 0x00, 0x01, 0x00,
        ]);
        packet.push(market);
        packet.extend_from_slice(&code_buf);

        let body = self.send_and_recv(&packet)?;
        parse_finance_info(&body, market, code)
    }

    /// 获取除权除息
    pub fn get_xdxr_info(&self, market: u8, code: &str) -> Result<Vec<XdXrInfo>> {
        let code_buf = utils::code_bytes(code);
        let mut packet = Vec::with_capacity(21);
        packet.extend_from_slice(&[
            0x0c, 0x1f, 0x18, 0x76, 0x00, 0x01, 0x0b, 0x00, 0x0b, 0x00, 0x0f, 0x00, 0x01, 0x00,
        ]);
        packet.push(market);
        packet.extend_from_slice(&code_buf);

        let body = self.send_and_recv(&packet)?;
        parse_xdxr_info(&body)
    }

    /// 获取板块元数据
    pub fn get_block_info_meta(&self, block_file: &str) -> Result<BlockInfoMeta> {
        let mut name_buf = [0u8; 40];
        let bytes = block_file.as_bytes();
        let len = bytes.len().min(40);
        name_buf[..len].copy_from_slice(&bytes[..len]);

        let mut packet = Vec::with_capacity(52);
        packet.extend_from_slice(&[
            0x0C, 0x39, 0x18, 0x69, 0x00, 0x01, 0x2A, 0x00, 0x2A, 0x00, 0xC5, 0x02,
        ]);
        packet.extend_from_slice(&name_buf);

        let body = self.send_and_recv(&packet)?;
        parse_block_info_meta(&body)
    }

    /// 获取板块数据
    pub fn get_block_info(
        &self,
        block_file: &str,
        start: u32,
        size: u32,
    ) -> Result<Vec<u8>> {
        let mut name_buf = [0u8; 100];
        let bytes = block_file.as_bytes();
        let len = bytes.len().min(100);
        name_buf[..len].copy_from_slice(&bytes[..len]);

        let mut packet = Vec::with_capacity(120);
        packet.extend_from_slice(&[
            0x0c, 0x37, 0x18, 0x6a, 0x00, 0x01, 0x6e, 0x00, 0x6e, 0x00, 0xb9, 0x06,
        ]);
        packet.extend_from_slice(&start.to_le_bytes());
        packet.extend_from_slice(&size.to_le_bytes());
        packet.extend_from_slice(&name_buf);

        let body = self.send_and_recv(&packet)?;
        parse_block_info(&body)
    }

    /// 获取并解析板块信息
    pub fn get_and_parse_block_info(
        &self,
        block_file: &str,
    ) -> Result<Vec<crate::reader::block::BlockRecord>> {
        let meta = self.get_block_info_meta(block_file)?;
        let chunk_size: u32 = 0x7530;
        let mut all_data = Vec::new();
        let mut offset = 0u32;

        while offset < meta.size {
            let read_size = chunk_size.min(meta.size - offset);
            let chunk = self.get_block_info(block_file, offset, read_size)?;
            all_data.extend_from_slice(&chunk);
            offset += read_size;
        }

        crate::reader::block::parse_block(&all_data)
    }
}
