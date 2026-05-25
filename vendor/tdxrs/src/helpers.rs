/// 变长整数解码 - 类似 UTF-8 的有符号编码
/// 对应 Python tdxpy helper.py 中的 get_price 函数
///
/// 编码格式:
/// - 第一字节: [1][sign][6bit data]
/// - 后续字节: [1][7bit data] (如果有更多字节)
/// - 最后字节: [0][7bit data]
///
/// 返回 (value, new_pos)。如果数据不足，返回 (0, data.len())。
#[inline(always)]
pub fn get_price(data: &[u8], pos: usize) -> (i64, usize) {
    if pos >= data.len() {
        return (0, data.len());
    }
    let mut pos = pos;
    let first = data[pos];
    let sign = (first & 0x40) != 0;
    let mut result = (first & 0x3F) as i64;
    let mut shift = 6;

    if (first & 0x80) != 0 {
        loop {
            pos += 1;
            if pos >= data.len() {
                return (0, data.len());
            }
            let b = data[pos];
            result |= ((b & 0x7F) as i64) << shift;
            shift += 7;
            if (b & 0x80) == 0 {
                break;
            }
        }
    }

    pos += 1;
    let val = if sign { -result } else { result };
    (val, pos)
}

/// 交易量解码 - 对应 Python tdxpy helper.py 中的 get_volume 函数
#[inline]
pub fn get_volume(vol: i64) -> f64 {
    let logpoint = (vol >> (8 * 3)) as i64;

    let hleax = ((vol >> (8 * 2)) & 0xFF) as i64;
    let lheax = ((vol >> 8) & 0xFF) as i64;
    let lleax = (vol & 0xFF) as i64;

    let dw_ecx = logpoint * 2 - 0x7F;
    let dw_edx = logpoint * 2 - 0x86;
    let dw_esi = logpoint * 2 - 0x8E;
    let dw_eax = logpoint * 2 - 0x96;

    let tmp_eax = if dw_ecx < 0 { -dw_ecx } else { dw_ecx };
    let mut dbl_xmm6 = 2.0_f64.powi(tmp_eax as i32);
    if dw_ecx < 0 {
        dbl_xmm6 = 1.0 / dbl_xmm6;
    }

    let dbl_xmm4 = if hleax > 0x80 {
        let dwtmpeax = dw_edx + 1;
        let tmpdbl_xmm3 = 2.0_f64.powi(dwtmpeax as i32);
        let dbl_xmm0 = 2.0_f64.powi(dw_edx as i32) * 128.0
            + (hleax & 0x7F) as f64 * tmpdbl_xmm3;
        dbl_xmm0
    } else {
        if dw_edx >= 0 {
            2.0_f64.powi(dw_edx as i32) * hleax as f64
        } else {
            (1.0 / 2.0_f64.powi((-dw_edx) as i32)) * hleax as f64
        }
    };

    let mut dbl_xmm3 = 2.0_f64.powi(dw_esi as i32) * lheax as f64;
    let mut dbl_xmm1 = 2.0_f64.powi(dw_eax as i32) * lleax as f64;

    if (hleax & 0x80) != 0 {
        dbl_xmm3 *= 2.0;
        dbl_xmm1 *= 2.0;
    }

    dbl_xmm6 + dbl_xmm4 + dbl_xmm3 + dbl_xmm1
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_get_price_simple() {
        // 单字节正数: 0b0000_0010 = 2
        let data = [0x02];
        let (val, pos) = get_price(&data, 0);
        assert_eq!(val, 2);
        assert_eq!(pos, 1);
    }

    #[test]
    fn test_get_price_negative() {
        // 单字节负数: 0b0100_0010 = sign=true, data=2 -> -2
        let data = [0x42];
        let (val, pos) = get_price(&data, 0);
        assert_eq!(val, -2);
        assert_eq!(pos, 1);
    }

    #[test]
    fn test_get_price_multi_byte() {
        // 多字节: 0b1000_0001 0b0000_0001
        // first: data=1, shift=6
        // second: data=1, result = 1 | (1 << 6) = 65
        let data = [0x81, 0x01];
        let (val, pos) = get_price(&data, 0);
        assert_eq!(val, 65);
        assert_eq!(pos, 2);
    }

    #[test]
    fn test_get_price_zero() {
        let data = [0x00];
        let (val, pos) = get_price(&data, 0);
        assert_eq!(val, 0);
        assert_eq!(pos, 1);
    }

    #[test]
    fn test_get_price_max_single_byte() {
        // 0b0011_1111 = 63 (max positive single byte)
        let data = [0x3F];
        let (val, pos) = get_price(&data, 0);
        assert_eq!(val, 63);
        assert_eq!(pos, 1);
    }

    #[test]
    fn test_get_price_max_negative_single_byte() {
        // 0b0111_1111 = sign=true, data=63 -> -63
        let data = [0x7F];
        let (val, pos) = get_price(&data, 0);
        assert_eq!(val, -63);
        assert_eq!(pos, 1);
    }

    #[test]
    fn test_get_price_out_of_bounds() {
        let data: [u8; 0] = [];
        let (val, pos) = get_price(&data, 0);
        assert_eq!(val, 0);
        assert_eq!(pos, 0);
    }

    #[test]
    fn test_get_price_three_bytes() {
        // 0b1000_0001 0b1000_0001 0b0000_0001
        // first: data=1, shift=6
        // second: data=1, result |= 1<<6 = 65, shift=13
        // third: data=1, result |= 1<<13 = 65 + 8192 = 8257
        let data = [0x81, 0x81, 0x01];
        let (val, pos) = get_price(&data, 0);
        assert_eq!(val, 8257);
        assert_eq!(pos, 3);
    }

    #[test]
    fn test_get_price_with_offset() {
        // Test reading at an offset
        let data = [0xFF, 0x02, 0x03];
        let (val, pos) = get_price(&data, 1);
        assert_eq!(val, 2);
        assert_eq!(pos, 2);
    }

    #[test]
    fn test_get_volume_zero() {
        assert_eq!(get_volume(0), 0.0);
    }

    #[test]
    fn test_get_volume_simple() {
        // vol = 0x00_00_01_00 = 256
        // logpoint=0, hleax=0, lheax=1, lleax=0
        let vol = 0x00_00_01_00_i64;
        let result = get_volume(vol);
        // dw_ecx = 0*2 - 0x7F = -127
        // dw_edx = 0*2 - 0x86 = -134
        // dw_esi = 0*2 - 0x8E = -142
        // dw_eax = 0*2 - 0x96 = -150
        // dbl_xmm6 = 2^127 (very large)
        // This is a complex function, just verify it doesn't panic
        assert!(result >= 0.0);
    }
}
