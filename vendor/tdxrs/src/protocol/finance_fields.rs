//! gpcw 584 字段 → 45 个核心财务指标英文名映射
//!
//! 字段索引基于 mootdx financial/columns.py (1-based)。
//! 所有值均为 TDX 原始 f32，未做单位转换。
//!
//! 用法:
//! ```ignore
//! use crate::protocol::finance_fields::extract_indicators;
//! let named = extract_indicators(&record.fields); // HashMap<&str, f64>
//! ```

use std::collections::HashMap;

/// 单个指标定义: (gpcw 1-based index, 英文名, 中文名)
type FieldDef = (usize, &'static str, &'static str);

/// 45 个核心财务指标
const INDICATORS: &[FieldDef] = &[
    // ═══ 每股指标 ═══
    (1,   "eps",                    "基本每股收益"),
    (2,   "deducted_eps",           "扣除非经常性损益每股收益"),
    (3,   "undistributed_ps",       "每股未分配利润"),
    (4,   "bvps",                   "每股净资产"),
    (5,   "capital_reserve_ps",     "每股资本公积金"),
    (7,   "ocf_ps",                 "每股经营现金流量"),

    // ═══ 盈利能力 ═══
    (6,   "roe_diluted",            "净资产收益率(摊薄)"),
    (199, "net_margin",             "销售净利率(%)"),
    (200, "roa",                    "总资产净利率(%)"),
    (202, "gross_margin",           "销售毛利率(%)"),
    (207, "ebit",                   "息税前利润(EBIT)"),
    (208, "ebitda",                 "息税折旧摊销前利润(EBITDA)"),
    (281, "roe_weighted",           "加权净资产收益率"),

    // ═══ 成长能力 ═══
    (183, "revenue_yoy",            "营业收入增长率(%)"),
    (184, "net_profit_yoy",         "净利润增长率(%)"),
    (185, "equity_yoy",             "净资产增长率(%)"),
    (187, "asset_yoy",              "总资产增长率(%)"),
    (189, "op_profit_yoy",          "营业利润增长率(%)"),
    (190, "deducted_eps_yoy",       "扣非每股收益同比(%)"),

    // ═══ 偿债能力 ═══
    (159, "current_ratio",          "流动比率"),
    (160, "quick_ratio",            "速动比率"),
    (162, "interest_coverage",      "利息保障倍数"),
    (166, "tangible_debt_ratio",    "有形资产净值债务率(%)"),
    (167, "equity_multiplier",      "权益乘数"),
    (210, "debt_ratio",             "资产负债率(%)"),

    // ═══ 现金流质量 ═══
    (219, "ocf_ps_v2",              "每股经营性现金流(元)"),
    (221, "cf_op_income_ratio",     "经营CF/经营净收益(%)"),
    (228, "cf_net_profit_ratio",    "经营CF/净利润比率"),
    (229, "cash_recovery_ratio",    "全部资产现金回收率"),

    // ═══ 营运能力 ═══
    (172, "receivable_turnover",    "应收账款周转率"),
    (173, "inventory_turnover",     "存货周转率"),
    (175, "asset_turnover",         "总资产周转率"),
    (179, "current_asset_turnover", "流动资产周转率"),

    // ═══ 规模因子 (TDX 原始值, 单位未转换) ═══
    (40,  "total_assets",           "资产总计"),
    (63,  "total_liabilities",      "负债合计"),
    (72,  "total_equity",           "所有者权益合计"),
    (74,  "revenue",                "营业收入"),
    (86,  "operating_profit",       "营业利润"),
    (95,  "net_profit_is",          "净利润(利润表)"),
    (96,  "parent_net_profit",      "归母净利润"),
    (107, "operating_cf",           "经营活动现金流量净额"),

    // ═══ 其他 ═══
    (238, "total_shares",           "总股本"),
    (308, "net_profit_1y",          "近一年归母净利润"),
    (319, "revenue_ttm",            "营业总收入TTM"),
    (320, "employees",              "员工总数"),
];

/// 用户提供的映射中存在重复/冲突的 gpcw 索引 (已按优先索引保留)
#[allow(dead_code)]
const DUPLICATE_INDICES: &[(usize, &str, &str)] = &[
    (134, "net_profit", "净利润(现金流量表) — 与 95 可能重复, 保留 95"),
    (197, "roe_diluted", "净资产收益率(摊薄) — 与 6 重复, 保留 6"),
    (232, "parent_net_profit", "归母净利润 — 与 96 重复, 保留 96"),
];

/// 已验证为 0 的字段 (000001 平安银行 2025Q3) — 银行股部分指标不适用
#[allow(dead_code)]
const BANK_ZERO_FIELDS: &[(&str, &str)] = &[
    ("current_ratio", "流动比率 — 银行无此分类"),
    ("quick_ratio", "速动比率 — 银行无此分类"),
    ("ebit", "EBIT — 待核实"),
    ("ebitda", "EBITDA — 待核实"),
    ("gross_margin", "毛利率 — 银行无此指标"),
    ("employees", "员工人数 — 待核实"),
];

/// 从 gpcw 584 字段数组中提取 45 个命名指标
///
/// `fields`: `FinancialRecord.fields` (584 个 f32, 0-based 索引)
///
/// 返回: `HashMap<&str, f64>` — 英文名 → TDX 原始值
pub fn extract_indicators(fields: &[f32]) -> HashMap<&'static str, f64> {
    let mut map = HashMap::with_capacity(INDICATORS.len());
    for &(idx, en_name, _zh_name) in INDICATORS {
        let val = fields.get(idx - 1).copied().unwrap_or(0.0);
        map.insert(en_name, val as f64);
    }
    map
}

/// 提取指标并附带中文名 (适合调试/展示)
pub fn extract_with_labels(fields: &[f32]) -> Vec<(&'static str, &'static str, f64)> {
    INDICATORS
        .iter()
        .map(|&(idx, en, zh)| {
            let val = fields.get(idx - 1).copied().unwrap_or(0.0);
            (en, zh, val as f64)
        })
        .collect()
}

/// 返回完整字段定义表 (用于生成文档或校验)
pub fn field_definitions() -> &'static [FieldDef] {
    INDICATORS
}

/// 检查给定 fields 数组是否能覆盖所有指标 (len >= max_index)
pub fn validate_fields_len(fields_len: usize) -> bool {
    let max_idx = INDICATORS.iter().map(|f| f.0).max().unwrap_or(0);
    fields_len >= max_idx
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_indicators_empty() {
        let fields = vec![0.0f32; 584];
        let result = extract_indicators(&fields);
        assert_eq!(result.len(), INDICATORS.len());
        assert!((result["eps"] - 0.0).abs() < 1e-10);
    }

    #[test]
    fn test_extract_indicators_values() {
        let mut fields = vec![0.0f32; 584];
        fields[0] = 1.5;   // idx 1 = eps
        fields[5] = 12.5;  // idx 6 = roe_diluted
        let result = extract_indicators(&fields);
        assert!((result["eps"] - 1.5).abs() < 1e-10);
        assert!((result["roe_diluted"] - 12.5).abs() < 1e-10);
    }

    #[test]
    fn test_validate_fields_len() {
        assert!(validate_fields_len(584));
        assert!(!validate_fields_len(100));
    }
}
