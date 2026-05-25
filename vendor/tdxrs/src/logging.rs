//! 轻量级日志 — 按级别输出到 stderr, 模块名作为前缀
//!
//! 控制: 环境变量 `TDXRS_LOG` = off|error|warn|info|debug
//! 默认: debug 编译 → debug, release 编译 → warn

use std::sync::atomic::{AtomicU8, Ordering};

pub const OFF: u8 = 0;
pub const ERROR: u8 = 1;
pub const WARN: u8 = 2;
pub const INFO: u8 = 3;
pub const DEBUG: u8 = 4;

static LEVEL: AtomicU8 = AtomicU8::new(WARN);

pub fn init() {
    let lvl = if let Ok(v) = std::env::var("TDXRS_LOG") {
        match v.to_lowercase().as_str() {
            "off" => OFF, "error" => ERROR, "warn" => WARN,
            "info" => INFO, "debug" => DEBUG, _ => WARN,
        }
    } else if cfg!(debug_assertions) {
        DEBUG
    } else {
        WARN
    };
    LEVEL.store(lvl, Ordering::Relaxed);
}

pub fn set_level(lvl: u8) {
    LEVEL.store(lvl.min(DEBUG), Ordering::Relaxed);
}

pub fn level_str(lvl: u8) -> &'static str {
    match lvl {
        ERROR => "E", WARN => "W", INFO => "I", DEBUG => "D", _ => ""
    }
}

/// 调用方的宏包装 — 编译期选择是否展开 format 参数
/// 用法: `logd!("mod", "msg {}", v);`  (debug)
///       `logi!("mod", "msg");`        (info)
#[macro_export]
macro_rules! logd {
    ($mod:expr, $($arg:tt)*) => {
        if $crate::logging::DEBUG <= $crate::logging::level() {
            eprintln!("[D] {}  {}", $mod, format!($($arg)*));
        }
    };
}
#[macro_export]
macro_rules! logi {
    ($mod:expr, $($arg:tt)*) => {
        if $crate::logging::INFO <= $crate::logging::level() {
            eprintln!("[I] {}  {}", $mod, format!($($arg)*));
        }
    };
}
#[macro_export]
macro_rules! logw {
    ($mod:expr, $($arg:tt)*) => {
        if $crate::logging::WARN <= $crate::logging::level() {
            eprintln!("[W] {}  {}", $mod, format!($($arg)*));
        }
    };
}
#[macro_export]
macro_rules! loge {
    ($mod:expr, $($arg:tt)*) => {
        if $crate::logging::ERROR <= $crate::logging::level() {
            eprintln!("[E] {}  {}", $mod, format!($($arg)*));
        }
    };
}

#[inline]
pub fn level() -> u8 { LEVEL.load(Ordering::Relaxed) }

// ================================================================
// 单元测试
// ================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_level() {
        init();
        // release build → WARN, debug build → DEBUG
        let lvl = level();
        assert!(lvl == WARN || lvl == DEBUG);
    }

    #[test]
    fn test_set_level() {
        set_level(OFF);
        assert_eq!(level(), OFF);
        set_level(ERROR);
        assert_eq!(level(), ERROR);
        set_level(INFO);
        assert_eq!(level(), INFO);
        set_level(DEBUG);
        assert_eq!(level(), DEBUG);
        // restore
        set_level(WARN);
    }

    #[test]
    fn test_level_str() {
        assert_eq!(level_str(ERROR), "E");
        assert_eq!(level_str(WARN), "W");
        assert_eq!(level_str(INFO), "I");
        assert_eq!(level_str(DEBUG), "D");
    }

    #[test]
    fn test_macros_dont_panic() {
        // 这些宏在运行时检查 level, 低 level 时不应输出
        set_level(OFF);
        logd!("test", "should not print");
        loge!("test", "should not print");
        logw!("test", "should not print");
        logi!("test", "should not print");
        set_level(DEBUG);
        logd!("test", "ok to print in test");
        set_level(WARN);
    }
}
