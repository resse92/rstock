pub mod bottom_trend_inflection;
pub mod common;
pub mod immortal_guidance;
pub mod limit_up_pullback;
pub mod limit_up_sideways;
pub mod morning_star;
pub mod multi_golden_cross;
pub mod multi_party_cannon;
pub mod resistance_breakout;
pub mod strategy_2560_selection;
pub mod strong_wash_weak_to_strong;
pub mod trend_acceleration_inflection;
pub mod trend_resonance_reversal;
pub mod trend_start;
pub mod w_bottom;

use crate::patterns::indicators::SeriesIndicators;
use crate::patterns::model::{BarSeries, PatternSignal};

pub trait PatternDetector: Send + Sync {
    fn id(&self) -> &'static str;
    fn detect(&self, series: &BarSeries, indicators: &SeriesIndicators) -> Option<PatternSignal>;
}

pub use bottom_trend_inflection::BottomTrendInflectionDetector;
pub use immortal_guidance::ImmortalGuidanceDetector;
pub use limit_up_pullback::LimitUpPullbackDetector;
pub use limit_up_sideways::LimitUpSidewaysDetector;
pub use morning_star::MorningStarDetector;
pub use multi_golden_cross::MultiGoldenCrossDetector;
pub use multi_party_cannon::MultiPartyCannonDetector;
pub use resistance_breakout::ResistanceBreakoutDetector;
pub use strategy_2560_selection::Strategy2560SelectionDetector;
pub use strong_wash_weak_to_strong::StrongWashWeakToStrongDetector;
pub use trend_acceleration_inflection::TrendAccelerationInflectionDetector;
pub use trend_resonance_reversal::TrendResonanceReversalDetector;
pub use trend_start::TrendStartDetector;
pub use w_bottom::WBottomDetector;

pub fn default_detectors() -> Vec<Box<dyn PatternDetector>> {
    vec![
        Box::new(BottomTrendInflectionDetector::default()),
        Box::new(LimitUpPullbackDetector::default()),
        Box::new(LimitUpSidewaysDetector::default()),
        Box::new(MorningStarDetector::default()),
        Box::new(MultiGoldenCrossDetector::default()),
        Box::new(MultiPartyCannonDetector::default()),
        Box::new(ResistanceBreakoutDetector::default()),
        Box::new(StrongWashWeakToStrongDetector::default()),
        Box::new(TrendAccelerationInflectionDetector::default()),
        Box::new(TrendResonanceReversalDetector::default()),
        Box::new(ImmortalGuidanceDetector::default()),
        Box::new(WBottomDetector::default()),
        Box::new(TrendStartDetector::default()),
        Box::new(Strategy2560SelectionDetector::default()),
    ]
}
