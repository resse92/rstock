use crate::error::{Result, TdxError};

/// Response header: 16 bytes little-endian <IIIHH
/// (seq, method, _, zip_size, unzip_size)
#[derive(Debug, Clone)]
pub struct ResponseHeader {
    pub seq: u32,
    pub method: u32,
    pub zip_size: u32,
    pub unzip_size: u32,
}

pub const RSP_HEADER_LEN: usize = 16;

impl ResponseHeader {
    pub fn parse(buf: &[u8]) -> Result<Self> {
        if buf.len() < RSP_HEADER_LEN {
            return Err(TdxError::ResponseParse("header too short".into()));
        }
        // <IIIHH: seq(u32), method(u32), _(u32), zip_size(u16), unzip_size(u16)
        let seq = u32::from_le_bytes([buf[0], buf[1], buf[2], buf[3]]);
        let method = u32::from_le_bytes([buf[4], buf[5], buf[6], buf[7]]);
        let _ = u32::from_le_bytes([buf[8], buf[9], buf[10], buf[11]]);
        let zip_size = u16::from_le_bytes([buf[12], buf[13]]) as u32;
        let unzip_size = u16::from_le_bytes([buf[14], buf[15]]) as u32;
        Ok(Self { seq, method, zip_size, unzip_size })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_header_basic() {
        // seq=1, method=2, reserved=0, zip_size=100, unzip_size=200
        let buf: [u8; 16] = [
            1, 0, 0, 0,    // seq = 1
            2, 0, 0, 0,    // method = 2
            0, 0, 0, 0,    // reserved
            100, 0,         // zip_size = 100
            200, 0,         // unzip_size = 200
        ];
        let header = ResponseHeader::parse(&buf).unwrap();
        assert_eq!(header.seq, 1);
        assert_eq!(header.method, 2);
        assert_eq!(header.zip_size, 100);
        assert_eq!(header.unzip_size, 200);
    }

    #[test]
    fn test_parse_header_large_values() {
        // seq=0xFFFFFFFF, method=0x12345678, zip_size=65535, unzip_size=1000
        let buf: [u8; 16] = [
            0xFF, 0xFF, 0xFF, 0xFF,
            0x78, 0x56, 0x34, 0x12,
            0, 0, 0, 0,
            0xFF, 0xFF,
            0xE8, 0x03,
        ];
        let header = ResponseHeader::parse(&buf).unwrap();
        assert_eq!(header.seq, 0xFFFFFFFF);
        assert_eq!(header.method, 0x12345678);
        assert_eq!(header.zip_size, 65535);
        assert_eq!(header.unzip_size, 1000);
    }

    #[test]
    fn test_parse_header_too_short() {
        let buf: [u8; 10] = [0; 10];
        let result = ResponseHeader::parse(&buf);
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_header_empty() {
        let result = ResponseHeader::parse(&[]);
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_header_equal_sizes() {
        // zip_size == unzip_size (no compression)
        let buf: [u8; 16] = [
            0, 0, 0, 0,
            0, 0, 0, 0,
            0, 0, 0, 0,
            0xE8, 0x03,  // 1000
            0xE8, 0x03,  // 1000
        ];
        let header = ResponseHeader::parse(&buf).unwrap();
        assert_eq!(header.zip_size, 1000);
        assert_eq!(header.unzip_size, 1000);
    }
}
