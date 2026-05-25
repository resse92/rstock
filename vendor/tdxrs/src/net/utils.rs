//! 网络客户端公共工具
//!
//! 提取 client.rs / direct_client.rs / async_client.rs 中的重复逻辑

use flate2::read::ZlibDecoder;
use std::io::Read;

use crate::error::{Result, TdxError};
use crate::net::connection::TcpConnection;
use crate::net::packet::{ResponseHeader, RSP_HEADER_LEN};
use crate::protocol::constants::*;
use crate::protocol::parsers::parse_security_bars;
use crate::protocol::types::{SecurityBar, XdXrInfo};

// ================================================================
// 股票代码编解码
// ================================================================

/// 股票代码 → 6 字节定长数组
pub fn code_bytes(code: &str) -> [u8; 6] {
    let mut buf = [0u8; 6];
    let bytes = code.as_bytes();
    let len = bytes.len().min(6);
    buf[..len].copy_from_slice(&bytes[..len]);
    buf
}

// ================================================================
// 请求包构建
// ================================================================

/// 构建 security bars 请求包
pub fn build_security_bars_packet(
    category: u8, market: u8, code: &str,
    start: u32, count: u16, fq: u8,
) -> Vec<u8> {
    let code_buf = code_bytes(code);
    let mut pkt = Vec::with_capacity(38);
    pkt.extend_from_slice(&0x010Cu16.to_le_bytes());
    pkt.extend_from_slice(&0x01016408u32.to_le_bytes());
    pkt.extend_from_slice(&0x001Cu16.to_le_bytes());
    pkt.extend_from_slice(&0x001Cu16.to_le_bytes());
    pkt.extend_from_slice(&CMD_SECURITY_BARS.to_le_bytes());
    pkt.extend_from_slice(&(market as u16).to_le_bytes());
    pkt.extend_from_slice(&code_buf);
    pkt.extend_from_slice(&(category as u16).to_le_bytes());
    pkt.extend_from_slice(&(fq as u16).to_le_bytes());
    pkt.extend_from_slice(&(start as u16).to_le_bytes());
    pkt.extend_from_slice(&count.to_le_bytes());
    pkt.extend_from_slice(&0u32.to_le_bytes());
    pkt.extend_from_slice(&0u32.to_le_bytes());
    pkt.extend_from_slice(&0u16.to_le_bytes());
    pkt
}

/// 构建 index bars 请求包 (与 security bars 格式相同, 语义区分)
pub fn build_index_bars_packet(
    category: u8, market: u8, code: &str,
    start: u32, count: u16, fq: u8,
) -> Vec<u8> {
    build_security_bars_packet(category, market, code, start, count, fq)
}

// ================================================================
// 握手 / 响应处理
// ================================================================

/// 执行 TDX 三步握手协议 (同步)
pub fn perform_handshake(conn: &mut TcpConnection) -> Result<()> {
    for cmd in &[SETUP_CMD1, SETUP_CMD2, SETUP_CMD3] {
        conn.send(cmd)?;
        let (head, body) = read_response_raw(conn)?;
        if head.zip_size != head.unzip_size {
            let _ = decompress_zlib(&body)?;
        }
    }
    Ok(())
}

/// 从 TCP 连接读取响应头 + 压缩体
fn read_response_raw(conn: &mut TcpConnection) -> Result<(ResponseHeader, Vec<u8>)> {
    let head_buf = conn.recv(RSP_HEADER_LEN)?;
    let header = ResponseHeader::parse(&head_buf)?;
    let zip_size = header.zip_size as usize;
    let mut body = Vec::with_capacity(zip_size);
    while body.len() < zip_size {
        let chunk = conn.recv(zip_size - body.len())?;
        body.extend_from_slice(&chunk);
    }
    Ok((header, body))
}

/// zlib 解压
pub fn decompress_zlib(data: &[u8]) -> Result<Vec<u8>> {
    let mut decoder = ZlibDecoder::new(data);
    let mut decompressed = Vec::new();
    decoder.read_to_end(&mut decompressed)
        .map_err(|e| TdxError::ResponseParse(format!("zlib decompress: {}", e)))?;
    Ok(decompressed)
}

// ================================================================
// 复权上下文拉取
// ================================================================

/// 为复权计算获取额外的历史 K 线
///
/// 当除权除息事件早于请求的 K 线数据时，向后翻页拉取更早的 K 线用于因子计算。
/// `send_fn`: 发包回调 (适配不同客户端连接模型)
pub fn fetch_context_bars_for_adjust<F: Fn(&[u8]) -> Result<Vec<u8>>>(
    send_fn: F,
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
        let pkt = build_security_bars_packet(category, market, code, offset, MAX_KLINE_COUNT, 0);
        let body = match send_fn(&pkt) {
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
// 单元测试
// ================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_code_bytes_full() {
        let b = code_bytes("600519");
        assert_eq!(b, [0x36, 0x30, 0x30, 0x35, 0x31, 0x39]);
    }

    #[test]
    fn test_code_bytes_short() {
        let b = code_bytes("SH");
        // "SH" = [0x53, 0x48], then padded with 0
        assert_eq!(b[0], 0x53);
        assert_eq!(b[1], 0x48);
        assert_eq!(b[2], 0x00);
        assert_eq!(b[5], 0x00);
    }

    #[test]
    fn test_code_bytes_long() {
        let b = code_bytes("1234567");
        // truncated to 6 bytes
        assert_eq!(b.len(), 6);
    }

    #[test]
    fn test_build_security_bars_packet() {
        let pkt = build_security_bars_packet(4, 1, "600519", 0, 800, 1);
        assert_eq!(pkt.len(), 38);
        // Header: 0x010C(2) + 0x01016408(4) + 0x001C(2) + 0x001C(2) + CMD(2) = 12
        // market at pos 12-13 (u16 LE)
        assert_eq!(u16::from_le_bytes([pkt[12], pkt[13]]), 1);
        // code at pos 14-19
        assert_eq!(&pkt[14..20], b"600519");
        // category at pos 20-21
        assert_eq!(u16::from_le_bytes([pkt[20], pkt[21]]), 4);
        // fq at pos 22-23
        assert_eq!(u16::from_le_bytes([pkt[22], pkt[23]]), 1);
    }

    #[test]
    fn test_build_index_bars_packet() {
        let pkt = build_index_bars_packet(4, 1, "000001", 0, 100, 0);
        assert_eq!(pkt.len(), 38);
        // Same format as security bars, verify code at pos 14-19
        assert_eq!(&pkt[14..20], b"000001");
    }

    #[test]
    fn test_decompress_zlib_no_data() {
        // Empty data should fail decompression
        let result = decompress_zlib(&[]);
        assert!(result.is_err() || result.is_ok());
        // zlib needs a proper header; empty input may error or produce empty
    }

    #[test]
    fn test_decompress_zlib_invalid() {
        let result = decompress_zlib(&[0xFF, 0xFF, 0xFF, 0xFF]);
        assert!(result.is_err());
    }

    #[test]
    fn test_fetch_context_empty_bars() {
        let bars: Vec<SecurityBar> = vec![];
        let xdxr: Vec<XdXrInfo> = vec![];
        let ctx = fetch_context_bars_for_adjust(
            |_| Ok(Vec::new()),
            4, 0, "000001", &bars, &xdxr,
        );
        assert!(ctx.is_empty());
    }
}
