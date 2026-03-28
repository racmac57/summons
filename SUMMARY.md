# Project Summary -- Summons ETL Pipeline

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

## Current State (2026-03-28)

The pipeline is operational but the repository has significant clutter: ~75 dead Python scripts, stale markdown documentation, empty junk files, and broken batch files. A reorganization proposal exists at `reorganization_proposal.md`. The two active ETL scripts (`SummonsMaster_Simple.py` and `summons_etl_enhanced.py`) both write the same output file, requiring the user to decide which to run.
