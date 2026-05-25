//! 裸连接客户端：每次请求新建 TCP 连接 + 三步握手，无连接池、无重试、无心跳
//!
//! 适用场景：偶发请求，不需要维护长连接

use crate::error::{Result, TdxError};
use crate::net::connection::TcpConnection;
use crate::net::packet::{ResponseHeader, RSP_HEADER_LEN};
use crate::net::utils;
use crate::protocol::constants::*;
use crate::protocol::parsers::*;
use crate::protocol::types::*;

/// 裸连接客户端
///
/// 不维护连接池，每次 API 调用都经历：新建 TCP → 三步握手 → 发包 → 收包 → 解压 → 断开
pub struct TdxDirectClient {
    ip: String,
    port: u16,
    timeout: f64,
}

impl TdxDirectClient {
    pub fn new(ip: &str, port: u16, timeout: f64) -> Self {
        Self {
            ip: ip.to_string(),
            port,
            timeout,
        }
    }

    /// 更新服务器地址
    pub fn set_server(&mut self, ip: &str, port: u16) {
        self.ip = ip.to_string();
        self.port = port;
    }

    /// 更新超时
    pub fn set_timeout(&mut self, timeout: f64) {
        self.timeout = timeout;
    }

    // ================================================================
    // 核心：send_and_recv
    // ================================================================

    fn send_and_recv(&self, packet: &[u8]) -> Result<Vec<u8>> {
        let mut conn = TcpConnection::connect(&self.ip, self.port, self.timeout)?;
        utils::perform_handshake(&mut conn)?;

        conn.send(packet)?;

        let head_buf = conn.recv(RSP_HEADER_LEN)?;
        let header = ResponseHeader::parse(&head_buf)?;

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

        if header.zip_size != header.unzip_size {
            utils::decompress_zlib(&body_buf)
        } else {
            Ok(body_buf)
        }
    }

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

    // ================================================================
    // K线
    // ================================================================

    pub fn get_security_bars(
        &self,
        category: u8,
        market: u8,
        code: &str,
        start: u32,
        count: u16,
        fq: u8,
    ) -> Result<Vec<SecurityBar>> {
        let pkt = utils::build_security_bars_packet(category, market, code, start, count, fq);
        let mut bars = parse_security_bars(&self.send_and_recv(&pkt)?, category)?;
        if fq != 0 {
            if let Ok(xdxr) = self.get_xdxr_info(market, code) {
                use crate::protocol::adjuster::{adjust_security_bars, FqType};
                let fq_enum = if fq == 2 { FqType::Hfq } else { FqType::Qfq };
                let context = self.fetch_context_bars_for_adjust(category, market, code, &bars, &xdxr);
                adjust_security_bars(&mut bars, &context, &xdxr, fq_enum);
            }
        }
        Ok(bars)
    }

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
        let pkt = utils::build_index_bars_packet(category, market, code, start, count, 0);
        let body = self.send_and_recv(&pkt)?;
        parse_index_bars(&body, category)
    }

    // ================================================================
    // 实时行情
    // ================================================================

    pub fn get_security_quotes(
        &self,
        all_stock: &[(u8, &str)],
    ) -> Result<Vec<SecurityQuote>> {
        let stock_len = all_stock.len() as u16;
        let pkgdatalen = (stock_len as u32) * 7 + 12;
        let mut pkt = Vec::with_capacity(26 + stock_len as usize * 7);
        pkt.extend_from_slice(&0x010Cu16.to_le_bytes());
        pkt.extend_from_slice(&0x02006320u32.to_le_bytes());
        pkt.extend_from_slice(&(pkgdatalen as u16).to_le_bytes());
        pkt.extend_from_slice(&(pkgdatalen as u16).to_le_bytes());
        pkt.extend_from_slice(&CMD_SECURITY_QUOTES.to_le_bytes());
        pkt.extend_from_slice(&0u32.to_le_bytes());
        pkt.extend_from_slice(&0u16.to_le_bytes());
        pkt.extend_from_slice(&stock_len.to_le_bytes());
        for &(market, code) in all_stock {
            pkt.push(market);
            pkt.extend_from_slice(&utils::code_bytes(code));
        }
        parse_security_quotes(&self.send_and_recv(&pkt)?)
    }

    // ================================================================
    // 证券信息
    // ================================================================

    pub fn get_security_list(&self, market: u8, start: u16) -> Result<Vec<SecurityInfo>> {
        let mut pkt = Vec::with_capacity(16);
        pkt.extend_from_slice(&[
            0x0c, 0x01, 0x18, 0x64, 0x01, 0x01, 0x06, 0x00, 0x06, 0x00, 0x50, 0x04,
        ]);
        pkt.extend_from_slice(&(market as u16).to_le_bytes());
        pkt.extend_from_slice(&start.to_le_bytes());
        parse_security_list(&self.send_and_recv(&pkt)?)
    }

    pub fn get_security_count(&self, market: u8) -> Result<u16> {
        let mut pkt = Vec::with_capacity(18);
        pkt.extend_from_slice(&[
            0x0c, 0x0c, 0x18, 0x6c, 0x00, 0x01, 0x08, 0x00, 0x08, 0x00, 0x4e, 0x04,
        ]);
        pkt.extend_from_slice(&(market as u16).to_le_bytes());
        pkt.extend_from_slice(&[0x75, 0xc7, 0x33, 0x01]);
        parse_security_count(&self.send_and_recv(&pkt)?)
    }

    // ================================================================
    // 分时数据
    // ================================================================

    pub fn get_minute_time_data(
        &self,
        market: u8,
        code: &str,
    ) -> Result<Vec<MinuteTimePrice>> {
        let code_buf = utils::code_bytes(code);
        let mut pkt = Vec::with_capacity(24);
        pkt.extend_from_slice(&[
            0x0c, 0x1b, 0x08, 0x00, 0x01, 0x01, 0x0e, 0x00, 0x0e, 0x00, 0x1d, 0x05,
        ]);
        pkt.extend_from_slice(&(market as u16).to_le_bytes());
        pkt.extend_from_slice(&code_buf);
        pkt.extend_from_slice(&0u32.to_le_bytes());
        parse_minute_time_data(&self.send_and_recv(&pkt)?, market, code)
    }

    pub fn get_history_minute_time_data(
        &self,
        market: u8,
        code: &str,
        date: u32,
    ) -> Result<Vec<MinuteTimePrice>> {
        let code_buf = utils::code_bytes(code);
        let mut pkt = Vec::with_capacity(23);
        pkt.extend_from_slice(&[
            0x0c, 0x01, 0x30, 0x00, 0x01, 0x01, 0x0d, 0x00, 0x0d, 0x00, 0xb4, 0x0f,
        ]);
        pkt.extend_from_slice(&date.to_le_bytes());
        pkt.push(market);
        pkt.extend_from_slice(&code_buf);
        parse_history_minute_time_data(&self.send_and_recv(&pkt)?, market, code)
    }

    // ================================================================
    // 逐笔成交
    // ================================================================

    pub fn get_transaction_data(
        &self,
        market: u8,
        code: &str,
        start: u16,
        count: u16,
    ) -> Result<Vec<TickData>> {
        let code_buf = utils::code_bytes(code);
        let mut pkt = Vec::with_capacity(24);
        pkt.extend_from_slice(&[
            0x0c, 0x17, 0x08, 0x01, 0x01, 0x01, 0x0e, 0x00, 0x0e, 0x00, 0xc5, 0x0f,
        ]);
        pkt.extend_from_slice(&(market as u16).to_le_bytes());
        pkt.extend_from_slice(&code_buf);
        pkt.extend_from_slice(&start.to_le_bytes());
        pkt.extend_from_slice(&count.to_le_bytes());
        parse_transaction_data(&self.send_and_recv(&pkt)?)
    }

    pub fn get_history_transaction_data(
        &self,
        market: u8,
        code: &str,
        start: u16,
        count: u16,
        date: u32,
    ) -> Result<Vec<TickData>> {
        let code_buf = utils::code_bytes(code);
        let mut pkt = Vec::with_capacity(28);
        pkt.extend_from_slice(&[
            0x0c, 0x01, 0x30, 0x01, 0x00, 0x01, 0x12, 0x00, 0x12, 0x00, 0xb5, 0x0f,
        ]);
        pkt.extend_from_slice(&date.to_le_bytes());
        pkt.extend_from_slice(&(market as u16).to_le_bytes());
        pkt.extend_from_slice(&code_buf);
        pkt.extend_from_slice(&start.to_le_bytes());
        pkt.extend_from_slice(&count.to_le_bytes());
        parse_history_transaction_data(&self.send_and_recv(&pkt)?)
    }

    // ================================================================
    // 财务 / 除权 / 板块
    // ================================================================

    pub fn get_finance_info(&self, market: u8, code: &str) -> Result<FinanceInfo> {
        let code_buf = utils::code_bytes(code);
        let mut pkt = Vec::with_capacity(21);
        pkt.extend_from_slice(&[
            0x0c, 0x1f, 0x18, 0x76, 0x00, 0x01, 0x0b, 0x00, 0x0b, 0x00, 0x10, 0x00, 0x01, 0x00,
        ]);
        pkt.push(market);
        pkt.extend_from_slice(&code_buf);
        parse_finance_info(&self.send_and_recv(&pkt)?, market, code)
    }

    pub fn get_xdxr_info(&self, market: u8, code: &str) -> Result<Vec<XdXrInfo>> {
        let code_buf = utils::code_bytes(code);
        let mut pkt = Vec::with_capacity(21);
        pkt.extend_from_slice(&[
            0x0c, 0x1f, 0x18, 0x76, 0x00, 0x01, 0x0b, 0x00, 0x0b, 0x00, 0x0f, 0x00, 0x01, 0x00,
        ]);
        pkt.push(market);
        pkt.extend_from_slice(&code_buf);
        parse_xdxr_info(&self.send_and_recv(&pkt)?)
    }

    // ================================================================
    // 板块信息
    // ================================================================

    /// 获取板块元数据
    pub fn get_block_info_meta(&self, block_file: &str) -> Result<BlockInfoMeta> {
        let mut name_buf = [0u8; 40];
        let bytes = block_file.as_bytes();
        let len = bytes.len().min(40);
        name_buf[..len].copy_from_slice(&bytes[..len]);

        let mut pkt = Vec::with_capacity(52);
        pkt.extend_from_slice(&[
            0x0C, 0x39, 0x18, 0x69, 0x00, 0x01, 0x2A, 0x00, 0x2A, 0x00, 0xC5, 0x02,
        ]);
        pkt.extend_from_slice(&name_buf);
        parse_block_info_meta(&self.send_and_recv(&pkt)?)
    }

    /// 获取板块数据
    pub fn get_block_info(&self, block_file: &str, start: u32, size: u32) -> Result<Vec<u8>> {
        let mut name_buf = [0u8; 100];
        let bytes = block_file.as_bytes();
        let len = bytes.len().min(100);
        name_buf[..len].copy_from_slice(&bytes[..len]);

        let mut pkt = Vec::with_capacity(120);
        pkt.extend_from_slice(&[
            0x0c, 0x37, 0x18, 0x6a, 0x00, 0x01, 0x6e, 0x00, 0x6e, 0x00, 0xb9, 0x06,
        ]);
        pkt.extend_from_slice(&start.to_le_bytes());
        pkt.extend_from_slice(&size.to_le_bytes());
        pkt.extend_from_slice(&name_buf);
        parse_block_info(&self.send_and_recv(&pkt)?)
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
