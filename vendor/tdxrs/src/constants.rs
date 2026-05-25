/// TDX 日期编码: year = num / 2048 + 2004, month = (num % 2048) / 100, day = (num % 2048) % 100
pub fn decode_date(num: u32) -> (u32, u32, u32) {
    let year = num / 2048 + 2004;
    let month = (num % 2048) / 100;
    let day = (num % 2048) % 100;
    (year, month, day)
}

/// TDX 日期编码 (u16 版本，用于分钟线)
pub fn decode_date_u16(num: u16) -> (u32, u32, u32) {
    let year = num as u32 / 2048 + 2004;
    let month = (num as u32 % 2048) / 100;
    let day = (num as u32 % 2048) % 100;
    (year, month, day)
}

/// TDX 时间解码: minutes since midnight -> (hour, minute)
pub fn decode_time(minutes: u16) -> (u32, u32) {
    (minutes as u32 / 60, minutes as u32 % 60)
}

/// 格式化日期为 "YYYY-MM-DD" 字符串
pub fn format_date(year: u32, month: u32, day: u32) -> String {
    format!("{:04}-{:02}-{:02}", year, month, day)
}

/// 格式化日期时间为 "YYYY-MM-DD HH:MM" 字符串
pub fn format_datetime(year: u32, month: u32, day: u32, hour: u32, minute: u32) -> String {
    format!("{:04}-{:02}-{:02} {:02}:{:02}", year, month, day, hour, minute)
}

/// 安全的字节切片索引
#[inline(always)]
pub fn get_byte(data: &[u8], pos: usize) -> u8 {
    data[pos]
}

/// 安全地读取 u32 (little-endian)
#[inline(always)]
pub fn read_u32(data: &[u8], pos: usize) -> u32 {
    if pos + 4 > data.len() {
        return 0;
    }
    u32::from_le_bytes([data[pos], data[pos + 1], data[pos + 2], data[pos + 3]])
}

/// 安全地读取 u16 (little-endian)
#[inline(always)]
pub fn read_u16(data: &[u8], pos: usize) -> u16 {
    if pos + 2 > data.len() {
        return 0;
    }
    u16::from_le_bytes([data[pos], data[pos + 1]])
}

/// 安全地读取 f32 (little-endian)
#[inline(always)]
pub fn read_f32(data: &[u8], pos: usize) -> f32 {
    f32::from_le_bytes([data[pos], data[pos + 1], data[pos + 2], data[pos + 3]])
}

/// 安全地读取 i32 (little-endian)
#[inline(always)]
pub fn read_i32(data: &[u8], pos: usize) -> i32 {
    i32::from_le_bytes([data[pos], data[pos + 1], data[pos + 2], data[pos + 3]])
}

/// 安全地读取 i64 (little-endian)
#[inline(always)]
pub fn read_i64(data: &[u8], pos: usize) -> i64 {
    i64::from_le_bytes([
        data[pos], data[pos + 1], data[pos + 2], data[pos + 3],
        data[pos + 4], data[pos + 5], data[pos + 6], data[pos + 7],
    ])
}
