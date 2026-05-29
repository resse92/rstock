pub mod cache;
pub mod detectors;
pub mod indicators;
pub mod model;
pub mod runner;

pub use cache::DuckDbPatternCache;
pub use detectors::{
    default_detectors, BottomTrendInflectionDetector, ImmortalGuidanceDetector,
    LimitUpPullbackDetector, LimitUpSidewaysDetector, MorningStarDetector,
    MultiGoldenCrossDetector, MultiPartyCannonDetector, PatternDetector,
    ResistanceBreakoutDetector, Strategy2560SelectionDetector, StrongWashWeakToStrongDetector,
    TrendAccelerationInflectionDetector, TrendResonanceReversalDetector, TrendStartDetector,
    WBottomDetector,
};
pub use model::{
    Bar, BarSeries, PatternCacheConfig, PatternScanReport, PatternScanRequest, PatternSignal,
};
pub use runner::{PatternDataSourceConfig, PatternScanner};
