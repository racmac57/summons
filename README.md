# Summons ETL Scripts

Python ETL pipeline for Hackensack Police Department summons data. Transforms e-ticket exports and historical court data into a Power BI staging file, and publishes a DFR (Drone / Directed Patrol Enforcement) workbook.

> **v2.3.0 (2026-04-14)** — Status-based retention is now live: officers who have separated stay in the lookup so their historical tickets resolve to a real name. Date-windowed assignment overrides (e.g., transitional duty) apply only to tickets in their date range. See `SUMMARY.md` and `HOW_TO_USE_ADD_TTD_WINDOW.md`.

## Primary Script: summons_etl_enhanced.py (entry: run_eticket_export.py)

The current production ETL. Processes:
- **Monthly e-ticket exports** (rolling 13-month window from `05_EXPORTS\_Summons\E_Ticket\`).
- **Historical backfill merge** for gap months prior to the e-ticket era.
- **Assignment Master join** — badge → officer display name, bureau, division. Both `ACTIVE` and `INACTIVE` rows are retained; `INACTIVE_REASON` is surfaced for downstream visibility.
- **`ASSIGNMENT_OVERRIDES`** static badge → assignment lookup (no date dimension; conditional rules like FIRE LANES live here).
- **`DFR_ASSIGNMENTS`** date-windowed overrides for transitional duty / temporary details — auto-split per calendar month via `scripts/summons_etl_normalize.py`.
- **DFR export** — filters to DFR badges and writes to the Drone workbook including `Violation Type` and `Fine Amount` (sourced directly from ETL values).

### Run

```bash
python run_eticket_export.py
python scripts/verify_summons_against_raw.py --report-month YYYY-MM
```

`SummonsMaster_Simple.py` is deprecated (archived 2026-03-28). Do not run it.

### E-Ticket Export Location

- Root: `05_EXPORTS\_Summons\E_Ticket\{year}\month\`
- Naming: `{YYYY}_{MM}_eticket_export.csv`
- The script auto-detects delimiter (semicolons for 2025, commas for 2026+)

### Output

- **Primary:** `03_Staging\Summons\summons_powerbi_latest.xlsx` (sheet: `Summons_Data`)
- **Log:** `summons_simple_processing.log`

## Other Active Scripts

| Script | Purpose |
|---|---|
| `run_eticket_export.py` | Production entry point — invokes `summons_etl_enhanced.py` |
| `summons_etl_enhanced.py` | Authoritative ETL (e-ticket + DFR + windowed overrides) |
| `summons_backfill_13month.py` | Rebuilds 13-month historical backfill |
| `scripts/summons_etl_normalize.py` | `normalize_date_windows` — month-segment splitter for `DFR_ASSIGNMENTS` |
| `scripts/verify_summons_against_raw.py` | Monthly raw-vs-staged reconciliation CLI |
| `update_dfr_violation_lookup.py` | Legacy DFR violation reference (no longer required for `Violation Type` / `Fine Amount` as of v2.3.0) |

## Key Business Rules

- Rolling 13-month window ending prior complete month
- `SUM(TICKET_COUNT)` in Power BI (never COUNTROWS -- aggregates have TICKET_COUNT > 1)
- Badge numbers zero-padded to 4 digits
- TYPE: M=Moving, P=Parking, C=Criminal

## Repository Structure

```
Summons/
  SummonsMaster_Simple.py      # Primary production ETL
  summons_etl_enhanced.py      # DFR-capable ETL variant
  summons_backfill_13month.py  # Historical backfill builder
  update_dfr_violation_lookup.py # DFR violation lookup updater
  run_eticket_export.py        # Test wrapper
  config.yaml                  # Legacy config (not actively loaded)
  summons_13month_trend.m      # Power Query: 13-month trend
  summons_top5_moving.m        # Power Query: Top 5 moving
  summons_top5_parking.m       # Power Query: Top 5 parking
  DAX/                         # Power BI DAX measures
  m_code/                      # Legacy M code versions (Oct-Nov 2025)
  backfill_data/               # Historical summary CSV
  archive/                     # Archived scripts and data
  docs/                        # Technical documentation
  documents/                   # Chat logs and development notes
```

## Documentation

- **CLAUDE.md** -- Full AI handoff document (authoritative)
- **docs/etl-pipeline.md** -- ETL pipeline technical reference
- **docs/file-inventory.md** -- Complete file inventory
- **docs/config-reference.md** -- Configuration variables reference
- **CHANGELOG.md** -- Version history
- **SUMMARY.md** -- Project summary
- **CONTRIBUTING.md** -- Contribution guidelines
