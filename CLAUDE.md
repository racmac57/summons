# Summons ETL Pipeline -- AI Handoff Document

## Project Purpose

This repository processes **police summons data** for the Hackensack Police Department's Power BI Summons Dashboard. It transforms raw e-ticket exports and historical court data into a single Excel staging file consumed by Power BI.

**Owner:** R. A. Carucci #261, Principal Analyst, SSOCC
**Cadence:** Monthly refresh (run after e-ticket export is dropped)
**Output:** `03_Staging\Summons\summons_powerbi_latest.xlsx` -> Power BI dashboard
**Remote:** `https://github.com/racmac57/summons.git` (branch: `main`)

## v2.3.0 Rules (Status-Based Retention — 2026-04-14)

- `_load_assignment_master` retains both ACTIVE and INACTIVE rows; `STATUS` and `INACTIVE_REASON` columns must be carried forward. Do not filter by `STATUS` upstream of the merge — INACTIVE rows are needed to resolve historical badges.
- Date-bounded assignments live in the `DFR_ASSIGNMENTS` list at module top of `summons_etl_enhanced.py`. They compose with the static `ASSIGNMENT_OVERRIDES` lookup (windows win on date overlap). Do not collapse the two — they have different semantics.
- `normalize_date_windows` (`scripts/summons_etl_normalize.py`) is the one-month-per-row guardrail for `DFR_ASSIGNMENTS`. Open-ended end dates are sentinel `None` and clipped to the data's max date at runtime; do not introduce far-future literal dates.
- New windows are added via the `/add-ttd-window` skill (`.claude/skills/add-ttd-window/SKILL.md`). Badge validation must allow legacy 1- and 2-digit badges (`#1`, `#15`) — never require `len >= 3`. Pad via `.zfill(4)`.
- `Violation Type` (DFR col S) and `Fine Amount` (DFR col T) are written directly from ETL values (`TYPE` post-PEO; `FINE_AMOUNT` numeric). Do not re-route them through `update_dfr_violation_lookup.py`.
- After every monthly run, `python scripts/verify_summons_against_raw.py --report-month YYYY-MM` is the publish gate. Exit 0 = clean to publish.

---

## End-to-End ETL Flow

```
DATA SOURCES
  +-- PowerBI_Data/Backfill/2025_12/summons/2025_12_department_wide_summons.csv
  |     (Aggregated historical data: Sep 2024 - Aug 2025)
  +-- 05_EXPORTS/_Summons/E_Ticket/{year}/month/{YYYY}_{MM}_eticket_export.csv
  |     (Individual monthly e-ticket records: Sep 2025+)
  +-- 09_Reference/Personnel/Assignment_Master_V2.csv
        (Badge -> Officer display name, bureau, division)

PROCESSING: summons_etl_enhanced.py  (AUTHORITATIVE -- production)
  1. Scan 13-month window of e-ticket CSVs (auto-detect delimiter)
  2. Validate Officer Id, pad to 4 digits, join to Assignment Master (V2/V3)
  3. Apply conditional overrides (ASSIGNMENT_OVERRIDES dict)
  4. FuzzyWuzzy last-name cross-validation
  5. Categorize violations, classify TYPE from Case Type Code
  6. Merge backfill for gap months via summons_backfill_merge
  7. DFR export: filter DFR badges, map to schema, append to Drone workbook

DEPRECATED: SummonsMaster_Simple.py  (moved to archive/deprecated/ 2026-03-28)
  Legacy script. See docs/etl-diff-analysis.md for full comparison.

OUTPUT: 03_Staging/Summons/summons_powerbi_latest.xlsx (sheet: Summons_Data)

POWER BI QUERIES (M Code):
  summons_13month_trend   -> 13-month bar chart
  summons_top5_moving     -> Top 5 officers (moving violations)
  summons_top5_parking    -> Top 5 officers (parking violations)
  summons_main_data       -> Base data table
  summons_all_bureaus     -> Bureau breakdown for prior month
```

---

## Active Production Files

| File | Lines | Purpose |
|---|---|---|
| `summons_etl_enhanced.py` | 833 | **AUTHORITATIVE -- current production script** -- 13-month e-ticket ETL + DFR export + FuzzyWuzzy matching |
| `archive/deprecated/SummonsMaster_Simple_DEPRECATED_2026-03-28.py` | 711 | **DEPRECATED -- do not run, do not delete pending confirmation** -- legacy backfill + e-ticket ETL |
| `summons_backfill_13month.py` | 213 | Builds 13-month backfill from historical e-ticket CSVs |
| `update_dfr_violation_lookup.py` | 206 | Updates ViolationData sheet in DFR workbook |
| `run_eticket_export.py` | 16 | One-line wrapper to test summons_etl_enhanced |

### Configuration

| File | Purpose | Status |
|---|---|---|
| `config.yaml` | Legacy config (paths, column mappings, thresholds) | Stale -- not loaded by active scripts |
| `emergency_config.yaml` | Fallback config with reduced paths | Stale -- emergency July 2025 artifact |

### M Code (Power Query) -- Production

| File | Query | Purpose |
|---|---|---|
| `summons_13month_trend.m` | summons_13month_trend | 13-month rolling trend (backfill + current) |
| `summons_top5_moving.m` | summons_top5_moving | Top 5 officers by moving violation count |
| `summons_top5_parking.m` | summons_top5_parking | Top 5 officers by parking violation count |
| `all_summons_queries_m_code.txt` | Multiple | Compiled reference of all Power BI queries |

### M Code -- Legacy (in `m_code/` directory)

The `m_code/` directory contains 18 files from Oct-Nov 2025. Files with `FINAL_` prefix were the production versions at that time. The root `.m` files (updated Jan 2026) supersede all `m_code/` files.

### DAX Measures

| File | Purpose |
|---|---|
| `DAX/TICKET_COUNT_MEASURES.dax` | SUM-based measures (Total, Moving, Parking, Complaints, MTD, Prior Month) |
| `DAX/Top_5_Moving_Subtitle` | Dynamic subtitle for Top 5 Moving card (uses COUNTROWS -- inconsistent with SUM rule) |

### Batch Files

| File | Purpose | Status |
|---|---|---|
| `SummonsMaster_ETL_Batch_Executor.bat` | Runs historical ETL (references defunct scripts first) | Dead -- references `simple_perfect_etl.py` |
| `Focused_ETL_Batch_Executor.bat` | Runs focused current-month ETL | Dead -- references `focused_summons_etl_current_month.py` (doesn't exist) |

### Key Reference Data

| File | Purpose |
|---|---|
| `backfill_data/25_08_Hackensack Police Department - Summons Dashboard.csv` | Historical summary (Sep 2024 - Aug 2025) |

---

## Key Fields and Join Logic

**Primary join:** `PADDED_BADGE_NUMBER` (4-digit zero-padded string) between ETL output and Assignment_Master_V2.csv.

| Field | Type | Source | Purpose |
|---|---|---|---|
| `PADDED_BADGE_NUMBER` | text(4) | ETL | Join key to Assignment Master |
| `OFFICER_DISPLAY_NAME` | text | Assignment Master | Display name in Power BI |
| `TYPE` | text | ETL classification | `M`=Moving, `P`=Parking, `C`=Criminal |
| `ETL_VERSION` | text | ETL tag | `HISTORICAL_SUMMARY` or `ETICKET_CURRENT` |
| `IS_AGGREGATE` | boolean | ETL flag | `true`=backfill aggregate, `false`=individual |
| `TICKET_COUNT` | int | ETL | >1 for aggregates, =1 for individuals |
| `YearMonthKey` | int | ETL | YYYYMM sort/filter key |
| `Month_Year` | text | ETL | `MM-YY` display label |
| `WG1` | text | Assignment Master | Division (e.g. OPERATIONS DIVISION) |
| `WG2` | text | Assignment Master | Bureau (e.g. PATROL BUREAU) |
| `WG3` | text | Assignment Master | **NOT in slim CSV or PBI** — only in deprecated script's ASSIGNMENT_OVERRIDES |
| `WG4` | text | Assignment Master | **NOT in slim CSV or PBI** — only in deprecated script |
| `WG5` | text | Assignment Master | **NOT in slim CSV or PBI** — only in deprecated script |
| `TEAM` | text | Assignment Master | **NOT in slim CSV or PBI** — only in deprecated script |
| `ISSUE_DATE` | date | Source CSV | Violation date |
| `TICKET_NUMBER` | text | Source CSV | Unique summons identifier |
| `VIOLATION_DESCRIPTION` | text | Source CSV | Statute/violation text |

---

## Business Rules and Assumptions

1. **Rolling 13-month window**: Always the 13 complete months ending with the previous complete month.
2. **Backfill/e-ticket split**: Pre-Sep 2025 = aggregated backfill (TICKET_COUNT > 1). Sep 2025+ = individual e-ticket records (TICKET_COUNT = 1).
3. **Power BI aggregation**: MUST use `SUM(TICKET_COUNT)`, never `COUNTROWS`, because aggregate records represent multiple tickets.
4. **Badge padding**: All badge numbers zero-padded to 4 digits (`38` -> `0038`).
5. **PEO/Class I reclassification rule** (`apply_peo_rule`): PEO and Class I officers cannot issue moving violations. Any record where WG3 is "PEO" or "CLASS I" and TYPE is "M" is reclassified to "P". Ported from SummonsMaster_Simple.py to summons_etl_enhanced.py on 2026-03-28. Runs after TYPE classification and before backfill merge.
6. **Conditional override**: Badge 2025 -> SSOCC only for "FIRE LANES" violations; other violations stay Traffic Bureau. (Was in ASSIGNMENT_OVERRIDES in deprecated SummonsMaster_Simple.py; not yet ported to enhanced script.)
7. **Delimiter detection**: 2025 e-ticket CSVs use semicolons; 2026+ use commas. Scripts auto-detect.
8. **TYPE classification**: Uses raw `Case Type Code` from export (M=Moving, P=Parking, C=Criminal). No statute-based reclassification except PEO/Class I rule (#5).

---

## Config, Paths, and Credentials References

All paths use the `carucci_r` junction form. **Do not change `carucci_r` to `RobertCarucci`** -- the junction resolves at runtime.

| Path | Purpose |
|---|---|
| `C:\Users\carucci_r\OneDrive - City of Hackensack\PowerBI_Data\Backfill\2025_12\summons\` | Backfill source |
| `C:\Users\carucci_r\OneDrive - City of Hackensack\05_EXPORTS\_Summons\E_Ticket\{year}\month\` | Monthly e-ticket exports |
| `C:\Users\carucci_r\OneDrive - City of Hackensack\09_Reference\Personnel\Assignment_Master_V2.csv` | Officer assignment lookup |
| `C:\Users\carucci_r\OneDrive - City of Hackensack\03_Staging\Summons\summons_powerbi_latest.xlsx` | Power BI staging output |
| `Shared Folder\Compstat\Contributions\Drone\dfr_directed_patrol_enforcement.xlsx` | DFR workbook (summons_etl_enhanced only) |

**No credentials or tokens are stored in this repository.**

---

## Refresh and Run Logic

### Monthly Refresh (Standard)

```bash
cd "C:\Users\carucci_r\OneDrive - City of Hackensack\02_ETL_Scripts\Summons"
python summons_etl_enhanced.py
```

**Prerequisites:**
1. New e-ticket export CSV placed in `05_EXPORTS/_Summons/E_Ticket/{year}/month/` with naming `{YYYY}_{MM}_eticket_export.csv`
2. `Assignment_Master_V2.csv` is current
3. Power BI is not holding a lock on `summons_powerbi_latest.xlsx`

**Post-run:** Refresh Power BI dataset. Verify counts in `summons_etl.log`. DFR records are automatically exported to the Drone workbook.

### Backfill Rebuild (Rare)

```bash
python summons_backfill_13month.py
```

Only needed if historical e-ticket data must be reprocessed from scratch.

---

## Safe Editing Rules for Claude

1. **Never rename columns** in output Excel -- Power BI M code queries reference exact column names.
2. **Never change sheet name** `Summons_Data` -- all M code queries reference it.
3. **Never change ETL_VERSION values** (`HISTORICAL_SUMMARY`, `ETICKET_CURRENT`) -- M code filters on these exact strings.
4. **Never change TYPE values** (`M`, `P`, `C`) -- DAX measures and M code filter on these.
5. **Never change `carucci_r`** to `RobertCarucci` in paths -- junction handles it.
6. **Never change `PowerBI_Data`** to `PowerBI_Date` -- the former is correct.
7. **ASSIGNMENT_OVERRIDES** dict is personnel-sensitive -- confirm changes with the user before modifying.
8. **Archive, don't delete** -- move obsolete files to `archive/` with datestamp per workspace conventions.
9. **Test with dry-run** before writing to staging -- log output counts and compare to prior month.
10. **Single production script** -- `summons_etl_enhanced.py` is the sole production script. `SummonsMaster_Simple.py` is deprecated (archived 2026-03-28).

---

## Validation Checklist

After each ETL run, verify:

- [ ] Output file exists: `03_Staging/Summons/summons_powerbi_latest.xlsx`
- [ ] Sheet name is `Summons_Data`
- [ ] Record count is in expected range (typically 4,000-8,000 for 13 months)
- [ ] 13 distinct `YearMonthKey` values present
- [ ] Both `HISTORICAL_SUMMARY` and `ETICKET_CURRENT` in `ETL_VERSION`
- [ ] `TYPE` column contains only `M`, `P`, `C`
- [ ] No blank `PADDED_BADGE_NUMBER` values in e-ticket records
- [ ] `TICKET_COUNT` sums to expected total (check log output)
- [ ] Current month individual records have `TICKET_COUNT = 1`
- [ ] Backfill aggregate records have `IS_AGGREGATE = true`

---

## Known Issues and Tech Debt

1. **~~Two active scripts produce the same output file.~~** RESOLVED 2026-03-28: `summons_etl_enhanced.py` is authoritative. `SummonsMaster_Simple.py` moved to `archive/deprecated/`. See `docs/etl-diff-analysis.md`.
2. **Hardcoded backfill path.** `SummonsMaster_Simple.py` references `2025_12_department_wide_summons.csv` -- this will need updating if a new backfill is generated.
3. **config.yaml is stale.** Uses `_EXPORTS` folder prefix and references `Assignment_Master.xlsm` (not V2.csv). No active script loads it.
4. **~75 dead Python scripts in root.** Prior cleanup archived copies but originals remain. See `reorganization_proposal.md`.
5. **Duplicate `(1)` files.** OneDrive sync conflicts created `README (1).md`, `SummonsMaster_Simple (1).py`, `README_SUMMONS_SCRIPTS (1).md`, `SOLUTION_SUMMARY (1).md`, `summons_etl_final (3).py`.
6. **Empty junk files.** Shell artifacts (`#`, `cd`, `echo`, `python`, `_ul`) in root -- created by misdirected terminal commands.
7. **M code versions scattered.** Root has current `.m` files; `m_code/` has 18 legacy versions. No clear indicator which supersedes which without reading dates.
8. **Batch files reference nonexistent scripts.** Both `.bat` files target scripts that no longer exist (`simple_perfect_etl.py`, `focused_summons_etl_current_month.py`).
9. **DAX inconsistency.** `Top_5_Moving_Subtitle` uses `COUNTROWS` instead of `SUM(TICKET_COUNT)`, contradicting the documented rule.
10. **`summons_backfill_13month.py` uses different Assignment Master path.** Points to `06_Workspace_Management/Assignment_Master_V2.csv` instead of `09_Reference/Personnel/Assignment_Master_V2.csv`.
11. **`run_eticket_export.py` uses `RobertCarucci` path.** Breaks the `carucci_r` junction convention.
12. **`process_monthly_summons.py` uses `C:\Dev\PowerBI_Data\Backfill`** -- a path that likely does not exist on the production machine.
13. **`summons_etl_enhanced.py` requires `fuzzywuzzy` package** -- not in any requirements.txt and not installable via standard means on all machines.
14. **No requirements.txt file.** `documents/requirements_txt.txt` exists but is not a proper pip requirements file at the repo root.
15. **Badge 0388 (LIGGIO)** — was hardcoded in ASSIGNMENT_OVERRIDES in the deprecated SummonsMaster_Simple.py as Patrol Bureau / Platoon A / A3. This override is NOT in summons_etl_enhanced.py. Badge 0388 appears in e-ticket data (Oct-Dec 2025) but may not be in Assignment_Master_V2.csv. **PENDING RAC confirmation**: Is LIGGIO still on this assignment? Should 0388 be added to the Assignment Master or to an override dict in the enhanced script?
16. **WG3/WG4/WG5/TEAM columns missing from pipeline.** The slim CSV and Power BI ___Summons table only have WG1 and WG2. The deprecated script populated WG3-WG5 and TEAM via ASSIGNMENT_OVERRIDES. If sub-bureau granularity is needed in PBI, these columns must be added to the enhanced ETL output.

---

## Working Conventions for Claude

- **Path junction:** `carucci_r` = `RobertCarucci` via Windows junction. Never change.
- **File naming:** `YYYY_MM_DD_short_description.ext`
- **Archive-first:** Never delete. Move to `archive/` with datestamp.
- **Case numbers:** Force `ReportNumberNew` to string dtype on Excel load (preserve `YY-NNNNNN` format).
- **Log file:** Check `summons_etl.log` for run diagnostics.
- **Workspace root:** `C:\Users\carucci_r\OneDrive - City of Hackensack\02_ETL_Scripts\Summons`

---

## Human Review Needed

1. **~~Which ETL is authoritative?~~** RESOLVED 2026-03-28: `summons_etl_enhanced.py` is authoritative. `SummonsMaster_Simple.py` deprecated and moved to `archive/deprecated/`.
2. **Is `config.yaml` loaded by anything?** It references old paths. May be safe to archive.
3. **Is `process_monthly_summons.py` still used?** It has different processing logic than `SummonsMaster_Simple.py`.
4. **Badge 0388 override** in `SummonsMaster_Simple.py` -- is LIGGIO still on this assignment?
5. **Badge 2025/0738 overrides** -- is the FIRE LANES conditional still active?
6. **Backfill data freshness** -- when will the `2025_12` backfill be superseded by a `2026_xx` backfill?
7. **Approve root cleanup** -- ~75 dead scripts and 15+ stale markdown files should be archived. See `reorganization_proposal.md`.

---

## Common User Questions

**Q: How do I add a new month's data?**
A: Drop the e-ticket CSV into `05_EXPORTS/_Summons/E_Ticket/{year}/month/` named `{YYYY}_{MM}_eticket_export.csv`, then run `python summons_etl_enhanced.py`.

**Q: Power BI shows wrong counts -- what's happening?**
A: Check if visuals use `COUNTROWS` instead of `SUM(TICKET_COUNT)`. Aggregate records have TICKET_COUNT > 1. See `DAX/TICKET_COUNT_MEASURES.dax` for correct measures.

**Q: A new officer isn't appearing in the dashboard.**
A: Update `Assignment_Master_V2.csv` with their badge number, then re-run the ETL. If they're a temporary assignment, add them to `ASSIGNMENT_OVERRIDES` in `summons_etl_enhanced.py`.

**Q: The 13-month trend shows a gap.**
A: Verify the e-ticket export exists for that month. Check `summons_simple_processing.log` for any file-not-found warnings.

**Q: How do I change the violation type classification?**
A: Look at the TYPE assignment logic in `summons_etl_enhanced.py`. TYPE is taken directly from the e-ticket export `Case Type Code` column (M=Moving, P=Parking, C=Criminal). See `_categorize_violations()` in the `SummonsETLProcessor` class.
