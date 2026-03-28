# TODO — Summons
## Generated from swarm audit 2026-03-28

### CRITICAL Priority
- [ ] ~~Dual ETL ambiguity~~ RESOLVED — `summons_etl_enhanced.py` is authoritative production script

### HIGH Priority
- [ ] Approve root cleanup: ~130 files should be archived/deleted (75 dead Python scripts, 22 stale MDs, 5 junk files, 5 OneDrive dupes). See reorganization_proposal.md
- [ ] Badge override review: Badge 0388 (LIGGIO) — is this assignment still current? Badge 2025/0738 FIRE LANES conditionals — still active?

### MEDIUM Priority
- [ ] config.yaml status: Not loaded by any active script. References old paths and old Assignment_Master format. Safe to archive?
- [ ] `process_monthly_summons.py`: Uses `C:\Dev\PowerBI_Data\Backfill` path. Still in use?
- [ ] Backfill freshness: When will the `2025_12` backfill be superseded by a `2026_xx` backfill?

### LOW Priority
- [ ] DAX inconsistency: `Top_5_Moving_Subtitle` uses COUNTROWS instead of SUM(TICKET_COUNT), contradicting documented aggregation rule
