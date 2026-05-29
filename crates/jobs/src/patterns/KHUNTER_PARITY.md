# KHunter Pattern Parity

Last updated after the detector parity pass on `2026-05-29`.

This file tracks parity between the Rust pattern detectors under `crates/jobs/src/patterns/`
and the reference strategy implementations under `KHunter/strategy/`.

Status legend:

- `near-parity`: core logic and major filters are aligned; only small edge cases or output-shape differences remain.
- `partial-parity`: main idea is implemented, but some KHunter-specific boundary logic still differs.
- `missing-system`: not a detector gap, but a KHunter support subsystem that is not implemented in Rust.

## Detector Table

| Rust detector | KHunter source | Status | Already aligned | Remaining differences | Next focus if needed |
| --- | --- | --- | --- | --- | --- |
| `bottom_trend_inflection` | `bottom_trend_inflection.py` | `partial-parity` | Deep decline, volume surge, distance-to-low, post-surge support | MACD divergence check is still an approximation via regression-style logic, not KHunter's exact flow | Tighten divergence rule with dedicated swing-point comparison |
| `immortal_guidance` | `immortal_guidance_strategy.py` | `partial-parity` | Signal-day search, upper-shadow threshold, trend filter, MA ordering, anti-body confirmation, confirmation payload | KHunter has extra freshness/truncation/lookup helpers and more explicit confirmation support checks | Add optional data-freshness guard and explicit post-confirmation stability window |
| `limit_up_pullback` | `limit_up_pullback_strategy.py` | `partial-parity` | Limit-up candidate scan, volume ratio, pullback window, support/resistance bounds, shrink-volume day, latest bullish confirmation | KHunter first collects candidate limit-up days then evaluates them with a more dataframe-oriented flow | Add candidate-list debug output and stricter pullback diagnostics |
| `limit_up_sideways` | `limit_up_sideways_strategy.py` | `near-parity` | Limit-up detection, sideways-day bounds, price band, support floor, shrink-volume check, KDJ/MACD breakout, volume expansion | Rust merges `_find_limit_up`, `_check_sideways`, `_check_breakout` into one detector; behavior is close but not step-for-step structured the same | Split into internal helper stages if easier debugging is needed |
| `morning_star` | `morning_star.py` | `near-parity` | Three-candle structure, first bearish body threshold, small middle body, third bullish confirmation, MA5 break, volume ratio | Mostly output-field and formatting differences from KHunter's signal object | Only adjust output contract if a consumer requires exact shape |
| `multi_golden_cross` | `multi_golden_cross.py` | `near-parity` | MA/KDJ/MACD cross detection, resonance window, latest price confirmation, volume check, key-date output | KHunter uses precomputed signal columns and scans the first valid cross in a dataframe-oriented way | Move cross events into a shared feature layer if batch analysis needs it |
| `multi_party_cannon` | `multi_party_cannon.py` | `near-parity` | Three-candle pattern, fallback ratio, shrink/expand volume, optional trend filters, category classification, reasons | KHunter organizes trend filters around indicator columns and has a more explicit vectorized path | Add a vectorized feature pass only if large-batch speed becomes a bottleneck |
| `resistance_breakout` | `resistance_breakout.py` | `near-parity` | Breakout-day search, volume expansion, resistance gap, MA bullish alignment, pullback support, reason output | Minor differences in how KHunter phrases reasons and packages signal metadata | Only align exact payload names if external consumers depend on them |
| `strategy_2560_selection` | `strategy_2560_selection.py` | `near-parity` | MA25 breakout, volume MA crossover, close above MA10, volume-ratio confirmation | KHunter's result object is a bit more report-like | No urgent logic work; only payload normalization if needed |
| `strong_wash_weak_to_strong` | `strong_wash_weak_to_strong.py` | `partial-parity` | Big candle, wash candle, reversal window, continued strength after reversal | KHunter has finer candle-edge checks and more exact sequencing details | Tighten wash/reversal candle body rules with explicit helper functions |
| `trend_acceleration_inflection` | `trend_acceleration_inflection.py` | `partial-parity` | Uptrend check, recent surge, distance from low, pullback support | KHunter's trend validation and data preprocessing are still more explicit | Move trend and support tests into reusable helpers for exact alignment |
| `trend_resonance_reversal` | `trend_resonance_reversal.py` | `near-parity` | RSI breakout, MA cross, MACD cross, resonance window, key-date output | Rust version is detector-native and does not mirror KHunter's dataframe search order exactly | Add event-level traces if auditability matters |
| `trend_start` | `trend_start_strategy.py` | `near-parity` | MACD cross above zero, Bollinger mid cross, bullish candle, MA5 confirmation, volume expansion | KHunter's signal builder has a more explicit payload struct and pattern tag | No major logic gap |
| `w_bottom` | `w_bottom_strategy.py` | `partial-parity` | Two local lows, gap threshold, neckline, breakout search, volume confirmation, fake-W filter, support hold, volume-shrink info | KHunter uses a more explicit LLV/window-based local-low search; Rust still uses a local-min heuristic | Replace low-point scan with LLV-style window logic to match KHunter more tightly |

## Non-detector gaps

These are present in KHunter but are not part of the Rust detector set yet:

| KHunter module | Status | Notes |
| --- | --- | --- |
| `pattern_feature_extractor.py` | `missing-system` | Rust currently computes shared indicators, but not a standalone reusable feature-extractor layer equivalent to KHunter's pattern feature object. |
| `pattern_library.py` | `missing-system` | No template/case library for pattern examples exists on the Rust side. |
| `pattern_matcher.py` | `missing-system` | No similarity-based pattern matcher or DTW-style shape match pipeline exists on the Rust side. |
| `parallel_strategy_executor.py` | `missing-system` | Rust has a runner, but not a feature-identical strategy executor abstraction matching KHunter's analysis pipeline. |
| `strategy_registry.py` | `missing-system` | Detectors are registered in code, but there is no KHunter-style runtime registry/parameter reload layer yet. |

## Suggested next steps

1. Add detector-level regression fixtures so each Rust detector can be checked against a frozen KHunter sample window.
2. Decide whether to stop at rule parity or also port the feature-library and pattern-matcher subsystems.
3. If exact KHunter behavior matters, prioritize `w_bottom`, `immortal_guidance`, `limit_up_pullback`, and `strong_wash_weak_to_strong` for another edge-case pass.
