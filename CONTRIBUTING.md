# Contributing

## Repository: Summons ETL Pipeline

**Owner:** R. A. Carucci #261, Principal Analyst, SSOCC, Hackensack Police Department

## Development Guidelines

1. **Archive, don't delete.** Move obsolete files to `archive/` with a datestamp. Never delete files outright.
2. **File naming.** Use `YYYY_MM_DD_short_description.ext` for new files.
3. **Path conventions.** Use `carucci_r` in all hardcoded paths (Windows junction resolves to `RobertCarucci`). Never change `carucci_r` to `RobertCarucci`.
4. **Output contract.** Do not rename columns, sheet names, ETL_VERSION values, or TYPE values in the staging output. Power BI M code and DAX measures depend on exact names.
5. **Test before writing.** Run ETL scripts with log review before overwriting `summons_powerbi_latest.xlsx`. Compare record counts to prior month.
6. **One output, one writer.** `SummonsMaster_Simple.py` and `summons_etl_enhanced.py` both write to the same output file. Run only one at a time.
7. **Personnel overrides.** Changes to `ASSIGNMENT_OVERRIDES` are personnel-sensitive. Confirm with the analyst before modifying.

## Branching

- `main` is the production branch
- Create feature branches for significant changes
- Push to `origin` (github.com/racmac57/summons.git)

## Commit Messages

Use conventional commit prefixes:
- `feat:` -- new feature or capability
- `fix:` -- bug fix
- `docs:` -- documentation only
- `chore:` -- maintenance, cleanup, config
- `refactor:` -- code restructuring without behavior change
