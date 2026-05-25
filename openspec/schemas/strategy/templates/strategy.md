## Purpose

<!-- Describe the market pattern, selection idea, or operational question this strategy answers. -->

## Input Data

<!--
Define the required K-line data:
- Frequency: daily, 1m, or another interval
- Source: local staging path, curated S3/MinIO prefix, CSV/ZIP import, or API
- Columns: symbol, timestamp/date, open, high, low, close, volume, amount, etc.
- Symbol universe: all stocks, index components, watchlist, or explicit list
- Date range and timezone
- Adjustment rules: forward-adjusted, backward-adjusted, unadjusted, or not applicable
- Missing/suspended/zero-volume bar handling
-->

## Calculations

<!--
List all derived fields and formulas.
Examples:
- ma_5 = rolling mean(close, 5)
- pct_chg_1d = close / previous_close - 1
- volume_ratio_5 = volume / rolling mean(volume, 5)

For each calculation, specify:
- window size
- warm-up period behavior
- null handling
- date alignment rule
-->

## Filters

<!--
Define screening conditions with exact thresholds and boolean logic.
Example:
- close > ma_20
- volume_ratio_5 >= 2.0
- pct_chg_1d between 0.03 and 0.095
- final_condition = condition_a AND condition_b AND NOT condition_c
-->

## Output

<!--
Define result shape and destination:
- Fields to output
- Sort order and tie-breakers
- Partitioning by date or strategy name
- File path, S3 prefix, CLI output, or API response
- Whether to include intermediate calculated fields
-->

## Validation

<!--
Define how to verify correctness:
- Formula unit cases
- Known sample symbols/dates
- Boundary dates and warm-up periods
- Missing data and suspended trading behavior
- Deterministic ordering
- Expected runtime or memory constraints
-->

## Risks / Limitations

<!-- Data-quality assumptions, false positives, known blind spots, and explicit non-goals. -->
