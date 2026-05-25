use crate::constants::{decode_date, format_date, read_f32, read_u32};
use crate::error::{Result, TdxError};

/// A股日线记录 (标准格式)
/// 格式: <IIIIIfII> = 32 bytes/record
/// date(u32), open(u32), high(u32), low(u32), close(u32), amount(f32), volume(u32), reserved(u32)
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct DailyBarRecord {
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
}

/// 解析 A股日线 .day 文件
///
/// 文件格式: 每条记录 32 字节，little-endian
/// - date: u32, TDX 日期编码
/// - open/high/low/close: u32, 整数价格 (需乘以 coefficient)
/// - amount: f32, 成交额
/// - volume: u32, 成交量
/// - reserved: u32, 保留字段
pub fn parse_daily_bar(data: &[u8], coefficient: f64) -> Result<Vec<DailyBarRecord>> {
    const RECORD_SIZE: usize = 32;

    if data.len() % RECORD_SIZE != 0 {
        return Err(TdxError::InvalidData(format!(
            "Daily bar file size {} is not a multiple of {}",
            data.len(),
            RECORD_SIZE
        )));
    }

    let count = data.len() / RECORD_SIZE;
    let mut records = Vec::with_capacity(count);

    for i in 0..count {
        let offset = i * RECORD_SIZE;
        let date_num = read_u32(data, offset);
        let open = read_u32(data, offset + 4) as f64 * coefficient;
        let high = read_u32(data, offset + 8) as f64 * coefficient;
        let low = read_u32(data, offset + 12) as f64 * coefficient;
        let close = read_u32(data, offset + 16) as f64 * coefficient;
        let amount = read_f32(data, offset + 20) as f64;
        let volume = read_u32(data, offset + 24) as f64;

        let (year, month, day) = decode_date(date_num);

        records.push(DailyBarRecord {
            date: format_date(year, month, day),
            open,
            high,
            low,
            close,
            amount,
            volume,
            year,
            month,
            day,
        });
    }

    Ok(records)
}

/// 读取日线文件并解析
pub fn read_daily_bar_file(filename: &str, coefficient: f64) -> Result<Vec<DailyBarRecord>> {
    let data = std::fs::read(filename)?;
    parse_daily_bar(&data, coefficient)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_daily_bar() {
        // 构造一条测试记录: 2024-01-15, open=10.50, high=10.80, low=10.30, close=10.60
        // date = (2024-2004)*2048 + 1*100 + 15 = 36979
        let date_num: u32 = (2024 - 2004) * 2048 + 1 * 100 + 15; // = 36979
        let open: u32 = 1050; // 10.50 * 100
        let high: u32 = 1080;
        let low: u32 = 1030;
        let close: u32 = 1060;
        let amount: f32 = 123456.5;
        let volume: u32 = 50000;
        let reserved: u32 = 0;

        let mut data = Vec::new();
        data.extend_from_slice(&date_num.to_le_bytes());
        data.extend_from_slice(&open.to_le_bytes());
        data.extend_from_slice(&high.to_le_bytes());
        data.extend_from_slice(&low.to_le_bytes());
        data.extend_from_slice(&close.to_le_bytes());
        data.extend_from_slice(&amount.to_le_bytes());
        data.extend_from_slice(&volume.to_le_bytes());
        data.extend_from_slice(&reserved.to_le_bytes());

        let records = parse_daily_bar(&data, 0.01).unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].date, "2024-01-15");
        assert!((records[0].open - 10.50).abs() < 1e-10);
        assert!((records[0].high - 10.80).abs() < 1e-10);
        assert!((records[0].low - 10.30).abs() < 1e-10);
        assert!((records[0].close - 10.60).abs() < 1e-10);
        assert_eq!(records[0].volume, 50000.0);
    }
}
