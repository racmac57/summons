# _apply_dfr_assignment_windows — Hardening Memory

**Path:** `summons_etl_enhanced.py` (Summons repo) — method of `SummonsETLProcessor`
**Type:** read-shape-preserving transform (returns new DataFrame; no side effects)
**Status:** PASS (9/9) — 2026-04-14

## Binary Scorecard

| # | Test | Result | Evidence |
|---|------|--------|----------|
| 1 | Exists & Loadable | 1 | Module imports clean; `DFR_ASSIGNMENTS` length 2 ("2025", "0115") |
| 2 | Shared Context Access | 1 | `normalize_date_windows` imports from `scripts/summons_etl_normalize.py`; `DFR_ASSIGNMENTS` constant resolves |
| 3 | Path Safety | 1 | No paths referenced in helper; config-driven |
| 4 | Data Dictionary Compliance | 1 | Uses `PADDED_BADGE_NUMBER`, `VIOLATION_DATE`, `OFFICER_DISPLAY_NAME`, `WG2` — canonical ETL column names |
| 5 | Idempotency | 1 | `r1.equals(r2)` over two invocations on identical input |
| 6 | Error Handling | 1 | Empty df → returns df; missing required columns → returns df unchanged; bad date ranges caught by `normalize_date_windows` |
| 7 | Output Correctness | 1 | Ramirez in-window row → `WG2=SSOCC`; outside-window → unchanged; Dominguez TTD row → `WG2=TTD`; unrelated badge untouched |
| 8 | CLAUDE.md Rule Compliance | 1 | Composes with `ASSIGNMENT_OVERRIDES` (windows win on overlap); does not filter STATUS (INACTIVE rows retained in lookup upstream) |
| 9 | Integration / Cross-Skill Safety | 1 | Integrated into `create_unified_summons_dataset`; end-to-end 13-month run produced correct outputs (73 ticket overrides across Ramirez March window) |

**Score:** 9/9 PASS

## Iteration History
- Initial impl crashed on end-of-data edge case: `date_end` clipped to `max_date` preceded `date_start` for Dominguez (TTD starts 2026-04-06, data ends 2026-04-01). Fixed by dropping windows whose `date_start > max_date` before splitting.
- Replaced Unicode `→` with `->` in log message (Windows cp1252 console).

## Regression Guards
- Unrelated row (badge `9999`) with no matching window must retain original `OFFICER_DISPLAY_NAME`/`WG2`. Asserted in test harness.
- Ramirez row dated outside window (2026-03-10) must not pick up `SSOCC` override.
