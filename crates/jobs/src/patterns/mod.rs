pub mod detectors;
pub mod indicators;
pub mod model;
pub mod runner;

pub use detectors::{
    default_detectors, BottomTrendInflectionDetector, ImmortalGuidanceDetector,
    LimitUpPullbackDetector, LimitUpSidewaysDetector, MorningStarDetector,
    MultiGoldenCrossDetector, MultiPartyCannonDetector, PatternDetector,
    ResistanceBreakoutDetector, Strategy2560SelectionDetector, StrongWashWeakToStrongDetector,
    TrendAccelerationInflectionDetector, TrendResonanceReversalDetector, TrendStartDetector,
    WBottomDetector,
};
pub use model::{Bar, BarSeries, PatternScanReport, PatternScanRequest, PatternSignal};
pub use runner::{PatternDataSourceConfig, PatternScanner};
