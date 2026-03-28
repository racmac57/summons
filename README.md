# Summons ETL Scripts

Python ETL pipeline for Hackensack Police Department summons data. Transforms e-ticket exports and historical court data into a Power BI staging file.

## Primary Script: SummonsMaster_Simple.py

The current production ETL. Processes:
- **Historical backfill** (Sep 2024 - Aug 2025 aggregates from `2025_12_department_wide_summons.csv`)
- **Monthly e-ticket exports** (Sep 2025+, individual records)
- **Assignment Master join** (badge -> officer display name, bureau, division)

### Run

```bash
python SummonsMaster_Simple.py
```

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
| `summons_etl_enhanced.py` | E-ticket ETL with DFR directed patrol export |
| `summons_backfill_13month.py` | Rebuilds 13-month historical backfill |
| `update_dfr_violation_lookup.py` | Updates DFR violation reference sheet |
| `run_eticket_export.py` | Test wrapper for summons_etl_enhanced |

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
