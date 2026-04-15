# Project Summary -- Summons ETL Pipeline

## v2.3.0 — Status-Based Retention (2026-04-14)

The pipeline now treats officer roster history as a first-class input, not a snapshot. Three coupled changes:

- **Historical badge resolution.** `_load_assignment_master` retains both `ACTIVE` and `INACTIVE` rows from `Assignment_Master_V2.csv` and surfaces `STATUS` + `INACTIVE_REASON` (Personnel v1.7.0). Tickets written by officers who have since resigned/retired/etc. resolve to a real display name instead of `UNKNOWN - XXXX`.
- **`DFR_ASSIGNMENTS` date-windowed overrides.** A new module-level list lets you say "this officer has assignment X between dates A and B" without rewriting the officer's other tickets. Open-ended windows (e.g., Lt. Dominguez TTD from 2026-04-06) are clipped to the data's max date and auto-split per calendar month by `normalize_date_windows` so Power BI month buckets stay clean.
- **Reconciliation CLI.** `python scripts/verify_summons_against_raw.py --report-month YYYY-MM` produces a one-screen receipt comparing staged vs raw e-ticket counts, badge counts, and INACTIVE resolutions. Exits non-zero if any INACTIVE badge from raw is still `UNKNOWN` in staging.

DFR exports also gained two first-class columns: `Violation Type` (col S, sourced from `TYPE` post-PEO reclassification) and `Fine Amount` (col T, from `FINE_AMOUNT`). No more dependency on `update_dfr_violation_lookup.py` for those values.

For everyday use of the windowed override system, see `HOW_TO_USE_ADD_TTD_WINDOW.md`.

---

## What It Does

Processes police summons (traffic, parking, ordinance violation citations) for the Hackensack Police Department. Raw e-ticket exports from the department's electronic citation system are merged with historical backfill data and officer assignment records to produce a single Excel staging file consumed by a Power BI dashboard.

## How It Works

1. **SummonsMaster_Simple.py** loads a historical backfill CSV (Sep 2024 - Aug 2025 aggregates) and scans for monthly e-ticket export CSVs (Sep 2025 onward).
2. Each record is tagged with `ETL_VERSION` (HISTORICAL_SUMMARY or ETICKET_CURRENT) and `TICKET_COUNT` (>1 for aggregates, =1 for individual tickets).
3. Badge numbers are zero-padded to 4 digits and joined to Assignment_Master_V2.csv to pull officer display names and organizational hierarchy (division, bureau, platoon, team).
4. A rolling 13-month window filter ensures only relevant data reaches Power BI.
5. Output goes to `03_Staging/Summons/summons_powerbi_latest.xlsx` (sheet: `Summons_Data`).

## Power BI Integration

Three M code queries read the staging file:
- **summons_13month_trend** -- 13-month bar chart of moving/parking/criminal summons
- **summons_top5_moving** -- Top 5 officers by moving violation count (current month)
- **summons_top5_parking** -- Top 5 officers by parking violation count (current month)

DAX measures in `DAX/TICKET_COUNT_MEASURES.dax` use `SUM(TICKET_COUNT)` (not COUNTROWS) to correctly handle aggregate records.

## Key Files

| File | Role |
|---|---|
| `SummonsMaster_Simple.py` | Primary production ETL |
| `summons_etl_enhanced.py` | Alternate ETL with DFR export |
| `summons_13month_trend.m` | Power Query: 13-month trend |
| `summons_top5_moving.m` | Power Query: Top 5 moving |
| `summons_top5_parking.m` | Power Query: Top 5 parking |
| `DAX/TICKET_COUNT_MEASURES.dax` | Power BI DAX measures |
| `CLAUDE.md` | Authoritative AI handoff document |

## Current State (2026-04-14)

The pipeline is in production with v2.3.0 status-based retention deployed. `summons_etl_enhanced.py` (entry point: `run_eticket_export.py`) is the authoritative ETL — `SummonsMaster_Simple.py` was deprecated and moved to `archive/deprecated/` on 2026-03-28. End-to-end validation for the 2026-03 reporting month: 4148/4160 rows staged, 64/64 badges, 0 remaining `UNKNOWN`, exit 0 from the reconciliation CLI.
