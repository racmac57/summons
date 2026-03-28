# Configuration Reference

## SummonsMaster_Simple.py -- Inline Config

Configuration is hardcoded at the top of the script (no external config file loaded).

| Variable | Value | Purpose |
|---|---|---|
| `BACKFILL_FILE_OVERRIDE` | `PowerBI_Data/Backfill/2025_12/summons/2025_12_department_wide_summons.csv` | Historical aggregate data |
| `BACKFILL_BASE_DIR` | `PowerBI_Data/Backfill` | Backfill root directory |
| `BACKFILL_FILENAME` | `2025_12_department_wide_summons.csv` | Backfill CSV filename |
| `ETICKET_FOLDER` | `05_EXPORTS/_Summons/E_Ticket` | Monthly e-ticket exports |
| `ASSIGNMENT_FILE` | `09_Reference/Personnel/Assignment_Master_V2.csv` | Officer lookup |
| `OUTPUT_FILE` | `03_Staging/Summons/summons_powerbi_latest.xlsx` | Power BI staging output |
| `LOG_FILE` | `summons_simple_processing.log` | Processing log |
| `TODAY_OVERRIDE` | `None` | Optional date override for testing |

## ASSIGNMENT_OVERRIDES (SummonsMaster_Simple.py)

Hardcoded dict for temporary/missing personnel:

| Badge | Officer | Condition | Assignment |
|---|---|---|---|
| `0388` | LIGGIO, PO | Always | PATROL BUREAU (Platoon A, A3) |
| `2025` | M. RAMIREZ #2025 | VIOLATION_DESCRIPTION contains "FIRE LANES" | SSOCC / PEO |
| `2025` | (default) | All other violations | Traffic Bureau (from Assignment Master) |

## ASSIGNMENT_OVERRIDES (summons_etl_enhanced.py)

| Badge | Officer | Condition | Assignment |
|---|---|---|---|
| `2025` | M. RAMIREZ #2025 | Violation Description contains "FIRE LANES" | SSOCC |
| `0738` | R. POLSON #0738 | Always | SSOCC |

## DFR_CONFIG (summons_etl_enhanced.py)

| Key | Value | Purpose |
|---|---|---|
| `dfr_badges` | `["0738", "2025"]` | Badges whose records go to DFR export |
| `target_workbook` | `Shared Folder/Compstat/.../dfr_directed_patrol_enforcement.xlsx` | DFR Excel output |
| `target_sheet` | `DFR Summons Log` | Target sheet name |
| `summons_prefix` | `E26` | Expected summons number prefix |
| `column_map` | B-Q mapping | Maps data columns to DFR sheet columns |
| `status_map` | ACTI->Active, VOID->Void, etc. | Status code translations |

## summons_backfill_13month.py -- Inline Config

| Variable | Value | Notes |
|---|---|---|
| `BASE` | `C:\Users\carucci_r\OneDrive - City of Hackensack` | OneDrive root |
| `ASSIGNMENT_MASTER` | `06_Workspace_Management/Assignment_Master_V2.csv` | Different path than primary script |
| `STAGING_CURRENT` | `03_Staging/Summons/summons_powerbi_latest.xlsx` | Reads current staging |
| `STAGING_OUT` | `03_Staging/Summons/summons_powerbi_latest.xlsx` | Overwrites same file |
| `HISTORICAL_FILES` | List of 12 hardcoded paths | Jan-Dec 2025 e-ticket CSVs |

## config.yaml (Legacy -- NOT LOADED)

Located at repository root. **No active script loads this file.**

Key sections:
- `data_sources` -- paths to assignment master (`Assignment_Master.xlsm`, outdated), court folder, ATS reports
- `outputs` -- staging output paths (some correct)
- `processing` -- `fuzzy_match_threshold: 75`, `badge_padding_length: 4`, `rolling_months: 12` (should be 13)
- `column_mappings` -- badge/officer/violation/date column name variations
- `validation` -- required columns, min records=10, max missing percentage=25

**Caution:** References `Assignment_Master.xlsm` (not V2.csv), `_EXPORTS` prefix, and `rolling_months: 12` (production uses 13). Paths may not match current folder structure.

## emergency_config.yaml (Legacy -- NOT LOADED)

Emergency deployment config from July 2025. References `Assignment_Master.xlsm` at `03_Staging/Summons/`. Contains `NOT_FOUND` placeholders for summons_master and ats_report.

## Path Resolution

All `carucci_r` paths resolve through Windows junction:
```
C:\Users\carucci_r  ->  C:\Users\RobertCarucci
C:\Users\RobertCarucci\OneDrive  ->  C:\Users\RobertCarucci\OneDrive - City of Hackensack
```

**Never change `carucci_r` to `RobertCarucci` in scripts.**

Exception: `run_eticket_export.py` currently uses `RobertCarucci` -- this is a bug.
