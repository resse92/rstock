use crate::constants::{decode_date_u16, decode_time, format_datetime, read_f32, read_u16, read_u32};
use crate::error::{Result, TdxError};

/// 分钟线记录 (整数格式 - TdxMinBarReader)
/// 格式: <HHIIIIfII> = 32 bytes/record
/// date(u16), time(u16), open(u32), high(u32), low(u32), close(u32), amount(f32), volume(u32), reserved(u32)
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct MinBarRecord {
    pub date: String,
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
    pub amount: f64,
    pub volume: f64,
    pub year: u32,
    pub month: u32,
    pub day: u32,
    pub hour: u32,
    pub minute: u32,
}

/// 分钟线记录 (浮点格式 - TdxLCMinBarReader)
/// 格式: <HHfffffII> = 32 bytes/record
/// date(u16), time(u16), open(f32), high(f32), low(f32), close(f32), amount(f32), volume(u32), reserved(u32)
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct LcMinBarRecord {
    pub date: String,
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
    pub amount: f64,
    pub volume: f64,
    pub year: u32,
    pub month: u32,
    pub day: u32,
    pub hour: u32,
    pub minute: u32,
}

/// 解析 5分钟线 .lc5/.lc1 文件 (整数格式)
/// OHLC 为整数，需除以 100
pub fn parse_min_bar(data: &[u8]) -> Result<Vec<MinBarRecord>> {
    const RECORD_SIZE: usize = 32;

    if data.len() % RECORD_SIZE != 0 {
        return Err(TdxError::InvalidData(format!(
            "Min bar file size {} is not a multiple of {}",
            data.len(),
            RECORD_SIZE
        )));
    }

    let count = data.len() / RECORD_SIZE;
    let mut records = Vec::with_capacity(count);

    for i in 0..count {
        let offset = i * RECORD_SIZE;
        let date_num = read_u16(data, offset);
        let time_num = read_u16(data, offset + 2);
        let open = read_u32(data, offset + 4) as f64 / 100.0;
        let high = read_u32(data, offset + 8) as f64 / 100.0;
        let low = read_u32(data, offset + 12) as f64 / 100.0;
        let close = read_u32(data, offset + 16) as f64 / 100.0;
        let amount = read_f32(data, offset + 20) as f64;
        let volume = read_u32(data, offset + 24) as f64;

        let (year, month, day) = decode_date_u16(date_num);
        let (hour, minute) = decode_time(time_num);

        records.push(MinBarRecord {
            date: format_datetime(year, month, day, hour, minute),
            open,
            high,
            low,
            close,
            amount,
            volume,
            year,
            month,
            day,
            hour,
            minute,
        });
    }

    Ok(records)
}

/// 解析分钟线 .lc5/.lc1 文件 (浮点格式)
/// OHLC 已是浮点，无需转换
pub fn parse_lc_min_bar(data: &[u8]) -> Result<Vec<LcMinBarRecord>> {
    const RECORD_SIZE: usize = 32;

    if data.len() % RECORD_SIZE != 0 {
        return Err(TdxError::InvalidData(format!(
            "LC min bar file size {} is not a multiple of {}",
            data.len(),
            RECORD_SIZE
        )));
    }

    let count = data.len() / RECORD_SIZE;
    let mut records = Vec::with_capacity(count);

    for i in 0..count {
        let offset = i * RECORD_SIZE;
        let date_num = read_u16(data, offset);
        let time_num = read_u16(data, offset + 2);
        let open = read_f32(data, offset + 4) as f64;
        let high = read_f32(data, offset + 8) as f64;
        let low = read_f32(data, offset + 12) as f64;
        let close = read_f32(data, offset + 16) as f64;
        let amount = read_f32(data, offset + 20) as f64;
        let volume = read_u32(data, offset + 24) as f64;

        let (year, month, day) = decode_date_u16(date_num);
        let (hour, minute) = decode_time(time_num);

        records.push(LcMinBarRecord {
            date: format_datetime(year, month, day, hour, minute),
            open,
            high,
            low,
            close,
            amount,
            volume,
            year,
            month,
            day,
            hour,
            minute,
        });
    }

    Ok(records)
}

/// 读取 5分钟线文件并解析
pub fn read_min_bar_file(filename: &str) -> Result<Vec<MinBarRecord>> {
    let data = std::fs::read(filename)?;
    parse_min_bar(&data)
}

/// 读取 LC 格式分钟线文件并解析
pub fn read_lc_min_bar_file(filename: &str) -> Result<Vec<LcMinBarRecord>> {
    let data = std::fs::read(filename)?;
    parse_lc_min_bar(&data)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_min_bar() {
        // 构造一条测试记录: 2024-01-15 09:35, open=10.50, high=10.80, low=10.30, close=10.60
        let date_num: u16 = ((2024 - 2004) * 2048 + 1 * 100 + 15) as u16;
        let time_num: u16 = 9 * 60 + 35; // 575 minutes
        let open: u32 = 1050; // 10.50 * 100
        let high: u32 = 1080;
        let low: u32 = 1030;
        let close: u32 = 1060;
        let amount: f32 = 123456.5;
        let volume: u32 = 5000;
        let reserved: u32 = 0;

        let mut data = Vec::new();
        data.extend_from_slice(&date_num.to_le_bytes());
        data.extend_from_slice(&time_num.to_le_bytes());
        data.extend_from_slice(&open.to_le_bytes());
        data.extend_from_slice(&high.to_le_bytes());
        data.extend_from_slice(&low.to_le_bytes());
        data.extend_from_slice(&close.to_le_bytes());
        data.extend_from_slice(&amount.to_le_bytes());
        data.extend_from_slice(&volume.to_le_bytes());
        data.extend_from_slice(&reserved.to_le_bytes());

        let records = parse_min_bar(&data).unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].date, "2024-01-15 09:35");
        assert!((records[0].open - 10.50).abs() < 1e-10);
        assert!((records[0].high - 10.80).abs() < 1e-10);
        assert_eq!(records[0].hour, 9);
        assert_eq!(records[0].minute, 35);
    }

    #[test]
    fn test_parse_lc_min_bar() {
        // 浮点格式: 2024-01-15 09:35, open=10.50 (直接浮点)
        let date_num: u16 = ((2024 - 2004) * 2048 + 1 * 100 + 15) as u16;
        let time_num: u16 = 575;
        let open: f32 = 10.50;
        let high: f32 = 10.80;
        let low: f32 = 10.30;
        let close: f32 = 10.60;
        let amount: f32 = 123456.5;
        let volume: u32 = 5000;
        let reserved: u32 = 0;

        let mut data = Vec::new();
        data.extend_from_slice(&date_num.to_le_bytes());
        data.extend_from_slice(&time_num.to_le_bytes());
        data.extend_from_slice(&open.to_le_bytes());
        data.extend_from_slice(&high.to_le_bytes());
        data.extend_from_slice(&low.to_le_bytes());
        data.extend_from_slice(&close.to_le_bytes());
        data.extend_from_slice(&amount.to_le_bytes());
        data.extend_from_slice(&volume.to_le_bytes());
        data.extend_from_slice(&reserved.to_le_bytes());

        let records = parse_lc_min_bar(&data).unwrap();
        assert_eq!(records.len(), 1);
        assert!((records[0].open - 10.50).abs() < 1e-10);
        assert!((records[0].high - 10.80).abs() < 1e-10);
    }
}
