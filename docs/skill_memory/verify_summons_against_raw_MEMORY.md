# verify_summons_against_raw.py — Hardening Memory

**Path:** `scripts/verify_summons_against_raw.py` (Summons repo)
**Type:** read-only verification CLI
**Status:** PASS (9/9) — 2026-04-14

## Binary Scorecard

| # | Test | Result | Evidence |
|---|------|--------|----------|
| 1 | Exists & Loadable | 1 | `py_compile` → exit 0 |
| 2 | Shared Context Access | 1 | Reads staging `summons_powerbi_latest.xlsx`, raw `05_EXPORTS/_Summons/E_Ticket/YYYY/month/*.csv`, Personnel `Assignment_Master_V2.csv` |
| 3 | Path Safety | 1 | Uses canonical `C:\Users\carucci_r\OneDrive - City of Hackensack` ROOT per global CLAUDE.md (`carucci_r` is required prefix) |
| 4 | Data Dictionary Compliance | 1 | References `PADDED_BADGE_NUMBER`, `OFFICER_DISPLAY_NAME`, `VIOLATION_DATE`, `STATUS`, `INACTIVE_REASON` — all current schema |
| 5 | Idempotency | 1 | Read-only; two invocations identical output |
| 6 | Error Handling | 1 | Missing raw file raises `FileNotFoundError` with clear message; process exits non-zero |
| 7 | Output Correctness | 1 | 2026-03 run: 4148/4160 rows, 64/64 badges, 0 UNKNOWN, exit 0 |
| 8 | CLAUDE.md Rule Compliance | 1 | Does not write to GOLD / CSV / SCHEMA; respects Personnel Rule 2 (no manual edits to generated files) |
| 9 | Integration / Cross-Skill Safety | 1 | Pure reader; no lock contention, no race conditions |

**Score:** 9/9 PASS

## Iteration History
- Initial design uses `carucci_r` root directly (not via `path_config.get_onedrive_root()`) for simplicity. Acceptable per global CLAUDE.md which states `carucci_r` is canonical on this desktop.

## Known Gotcha
- The `raw_path()` helper tries `_eticket_export.csv` then `_eticket_export_FIXED.csv`. A stale `_FIXED.csv` from a prior month could silently mask the canonical file — verify staging flow keeps only one in place per month.
