use serde::Serialize;

// ============================================================
// K线数据
// ============================================================

#[derive(Debug, Clone, Serialize)]
pub struct SecurityBar {
    pub open: f64,
    pub close: f64,
    pub high: f64,
    pub low: f64,
    pub vol: f64,
    pub amount: f64,
    pub year: u32,
    pub month: u32,
    pub day: u32,
    pub hour: u32,
    pub minute: u32,
    pub datetime: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct IndexBar {
    pub open: f64,
    pub close: f64,
    pub high: f64,
    pub low: f64,
    pub vol: f64,
    pub amount: f64,
    pub year: u32,
    pub month: u32,
    pub day: u32,
    pub hour: u32,
    pub minute: u32,
    pub datetime: String,
    pub up_count: u32,
    pub down_count: u32,
}

// ============================================================
// 实时行情
// ============================================================

#[derive(Debug, Clone, Serialize)]
pub struct SecurityQuote {
    pub market: u8,
    pub code: String,
    pub active1: u16,
    pub price: f64,
    pub last_close: f64,
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub servertime: String,
    pub vol: f64,
    pub cur_vol: f64,
    pub amount: f64,
    pub s_vol: f64,
    pub b_vol: f64,
    pub bid1: f64,
    pub bid_vol1: f64,
    pub bid2: f64,
    pub bid_vol2: f64,
    pub bid3: f64,
    pub bid_vol3: f64,
    pub bid4: f64,
    pub bid_vol4: f64,
    pub bid5: f64,
    pub bid_vol5: f64,
    pub ask1: f64,
    pub ask_vol1: f64,
    pub ask2: f64,
    pub ask_vol2: f64,
    pub ask3: f64,
    pub ask_vol3: f64,
    pub ask4: f64,
    pub ask_vol4: f64,
    pub ask5: f64,
    pub ask_vol5: f64,
    pub reversed_bytes0: u32,
    pub reversed_bytes1: u32,
    pub reversed_bytes2: u32,
    pub reversed_bytes3: u32,
    pub reversed_bytes4: u32,
    pub reversed_bytes5: u32,
    pub reversed_bytes6: u32,
    pub reversed_bytes7: u32,
    pub reversed_bytes8: u32,
    pub reversed_bytes9: u32,
    pub active2: u16,
}

// ============================================================
// 证券列表
// ============================================================

#[derive(Debug, Clone, Serialize)]
pub struct SecurityInfo {
    pub code: String,
    pub volunit: u16,
    pub decimal_point: u8,
    pub name: String,
    pub pre_close: f64,
}

// ============================================================
// 分时数据
// ============================================================

#[derive(Debug, Clone, Serialize)]
pub struct MinuteTimePrice {
    pub price: f64,
    pub vol: f64,
}

// ============================================================
// 逐笔成交
// ============================================================

#[derive(Debug, Clone, Serialize)]
pub struct TickData {
    pub time: String,
    pub price: f64,
    pub vol: f64,
    pub num: u32,
    pub buyorsell: u32,
}

// ============================================================
// 财务信息
// ============================================================

#[derive(Debug, Clone, Serialize)]
pub struct FinanceInfo {
    pub market: u8,
    pub code: String,
    pub liutongguben: f64,
    pub province: u16,
    pub industry: u16,
    pub updated_date: u32,
    pub ipo_date: u32,
    pub zongguben: f64,
    pub guojiagu: f64,
    pub faqirenfarengu: f64,
    pub farengu: f64,
    pub bgu: f64,
    pub hgu: f64,
    pub zhigonggu: f64,
    pub zongzichan: f64,
    pub liudongzichan: f64,
    pub gudingzichan: f64,
    pub wuxingzichan: f64,
    pub gudongrenshu: f64,
    pub liudongfuzhai: f64,
    pub changqifuzhai: f64,
    pub zibengongjijin: f64,
    pub jingzichan: f64,
    pub zhuyingshouru: f64,
    pub zhuyinglirun: f64,
    pub yingshouzhangkuan: f64,
    pub yingyelirun: f64,
    pub touzishouyu: f64,
    pub jingyingxianjinliu: f64,
    pub zongxianjinliu: f64,
    pub cunhuo: f64,
    pub lirunzonghe: f64,
    pub shuihoulirun: f64,
    pub jinglirun: f64,
    pub weifenpeilirun: f64,
    pub meigujingzichan: f64,
}

// ============================================================
// 除权除息
// ============================================================

#[derive(Debug, Clone, Serialize)]
pub struct XdXrInfo {
    pub year: u32,
    pub month: u32,
    pub day: u32,
    pub category: u32,
    pub name: String,
    pub fenhong: Option<f64>,
    pub peigujia: Option<f64>,
    pub songzhuangu: Option<f64>,
    pub peigu: Option<f64>,
    pub suogu: Option<f64>,
    pub panqianliutong: Option<f64>,
    pub panhouliutong: Option<f64>,
    pub qianzongguben: Option<f64>,
    pub houzongguben: Option<f64>,
    pub fenshu: Option<f64>,
    pub xingquanjia: Option<f64>,
}

// ============================================================
// 板块元数据
// ============================================================

#[derive(Debug, Clone, Serialize)]
pub struct BlockInfoMeta {
    pub size: u32,
    pub hash_value: String,
}

// ============================================================
// 证券类型和系数
// ============================================================

pub fn get_security_type(market: u8, code: &str) -> u8 {
    if market == 1 {
        // 上海
        if code.starts_with('5') {
            return 0; // 指数
        }
        if code.starts_with("60") || code.starts_with("68") {
            return 1; // A股
        }
        if code.starts_with("90") {
            return 2; // B股
        }
        if code.starts_with("51") {
            return 3; // 基金
        }
        if code.starts_with("11") || code.starts_with("13") {
            return 4; // 债券
        }
    } else if market == 0 {
        // 深圳
        if code.starts_with("39") {
            return 0; // 指数
        }
        if code.starts_with("00") || code.starts_with("30") {
            return 1; // A股
        }
        if code.starts_with("20") {
            return 2; // B股
        }
        if code.starts_with("15") || code.starts_with("16") {
            return 3; // 基金
        }
        if code.starts_with("10") || code.starts_with("12") || code.starts_with("13") {
            return 4; // 债券
        }
    }
    1 // 默认 A股
}

/// 获取价格系数
pub fn get_security_coefficient(market: u8, code: &str) -> f64 {
    let sec_type = get_security_type(market, code);
    match sec_type {
        0 => 0.01,  // 指数
        1 => 0.01,  // A股
        2 => 0.001, // B股
        3 => 0.001, // 基金
        4 => 0.0001, // 债券
        _ => 0.01,
    }
}
