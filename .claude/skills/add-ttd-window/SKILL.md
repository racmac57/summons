---
name: add-ttd-window
description: Insert a date-bounded DFR assignment override (TTD, SSOCC detail, admin leave, etc.) into DFR_ASSIGNMENTS in summons_etl_enhanced.py. Use when an officer is moved onto or off a transitional duty, extended detail, or temporary assignment whose dates matter for DFR reporting.
disable-model-invocation: true
argument-hint: --badge <N> --assignment <WG2> --from <YYYY-MM-DD> (--to <YYYY-MM-DD> | --open-ended) --reason <ENUM> [--dry-run] [--run-etl]
allowed-tools: Read Edit Grep Glob Bash(python *)
---

# Add DFR Assignment Window

Automate entering a temporary transitional duty (TTD) or other time-bounded
assignment override into `summons_etl_enhanced.py`'s `DFR_ASSIGNMENTS` list so
it applies to the correct date range without manual code editing.

## Arguments

All flags required unless marked optional. Order doesn't matter; parse from
`$ARGUMENTS`.

- `--badge <N>` — Officer badge number. Numeric, up to 4 digits; HPD has
  legacy single- and double-digit badges (`#1`, `#15`, `#27`) so do **not**
  require ≥3 digits. Will be zero-padded to 4 via `.zfill(4)`.
- `--assignment <WG2>` — Assignment code that lands in the `WG2` column
  (e.g. `TTD`, `SSOCC`, `ADMIN_LEAVE`, `LIGHT_DUTY`). Uppercase, no spaces.
- `--from <YYYY-MM-DD>` — Inclusive start date.
- `--to <YYYY-MM-DD>` — Inclusive end date. Mutually exclusive with `--open-ended`.
- `--open-ended` — Window has no end date yet (use when an officer is moving
  to TTD indefinitely). Sets `date_end = None`.
- `--reason <ENUM>` — Uppercase reason tag. Existing values:
  `MARCH_WINDOW`, `TRANSITIONAL_DUTY`. Add a new one if none fits.
- `--dry-run` *(optional)* — Print the proposed insertion and diff but do NOT
  edit the file.
- `--run-etl` *(optional)* — After a successful insert, run
  `python run_eticket_export.py` so the user can validate the override on the
  next staging build.

## Example Invocations

```
/add-ttd-window --badge 0115 --assignment TTD --from 2026-04-06 --open-ended --reason TRANSITIONAL_DUTY
/add-ttd-window --badge 2025 --assignment SSOCC --from 2026-03-01 --to 2026-03-04 --reason MARCH_WINDOW --dry-run
/add-ttd-window --badge 0421 --assignment LIGHT_DUTY --from 2026-05-10 --to 2026-06-10 --reason LIGHT_DUTY --run-etl
```

## Step 1 — Parse & Validate Arguments

1. Parse `$ARGUMENTS` into the flags above. Abort with a clear error on:
   - Missing required flag.
   - `--to` AND `--open-ended` both present.
   - Badge not numeric, or longer than 4 digits.
   - Date not in `YYYY-MM-DD`.
   - `--to` earlier than `--from`.

2. Zero-pad the badge to 4 digits (`"1"` → `"0001"`, `"115"` → `"0115"`).
   Keep it as a string. Rely on `.zfill(4)` — never assume a minimum
   original length.

## Step 2 — Look Up Officer in Assignment Master

Read the Personnel CSV to derive the officer's display name and confirm the
badge is known:

```
C:\Users\carucci_r\OneDrive - City of Hackensack\09_Reference\Personnel\Assignment_Master_V2.csv
```

Match on `PADDED_BADGE_NUMBER`. Pull the `STANDARD_NAME` column (format:
`"L. LASTNAME #NNNN"`). That becomes `OFFICER_DISPLAY_NAME` in the new entry.

- **If badge not found in Personnel CSV** — abort with:
  `"Badge {NNNN} not found in Assignment_Master_V2.csv. Update Personnel GOLD first, then run `python scripts/sync_assignment_master.py` in the Personnel repo before retrying."`
- **If multiple matches** — should be impossible (PADDED_BADGE_NUMBER is
  unique by contract); if it happens, abort and tell the user.

## Step 3 — Load Current DFR_ASSIGNMENTS & Check for Overlap

Read `summons_etl_enhanced.py` and locate the `DFR_ASSIGNMENTS = [ ... ]` list
(module-level, currently around lines 58–75). Parse the existing entries
(Python-literal parsing is fine — entries are simple dicts with string/None
values).

For each existing entry whose `badge` equals the new badge:
- Compute `[existing_start, existing_end]` (using a far-future sentinel for
  `None`) and `[new_start, new_end]`.
- If intervals overlap, WARN and show the offending entry. Ask the user to
  confirm before inserting (overlap is not an automatic abort — sometimes a
  second, tighter override is intentional; windows compose in insertion
  order).

## Step 4 — Build the New Entry

Construct the dict literal exactly in the style used in the file:

```python
    {
        "badge": "{padded_badge}",
        "date_start": "{from_date}",
        "date_end": {to_date_or_none},
        "WG2": "{assignment}",
        "OFFICER_DISPLAY_NAME": "{standard_name}",
        "reason": "{reason}",
    },
```

- `date_end` is either the quoted `YYYY-MM-DD` string or the bare `None`
  literal (no quotes).
- Match the file's existing indentation: 4 spaces.
- Inline comment `# open-ended TTD` on `date_end` when `--open-ended`,
  mirroring the Dominguez entry style.

## Step 5 — Insert & Diff

Insert the new dict as the last element of the list (just before the
closing `]`). Use `Edit` — the closing bracket is unique enough in the
`DFR_ASSIGNMENTS` block to anchor the edit, or match the full preceding
entry's trailing comma + newline.

Show the user a unified diff of the change (before/after of the
`DFR_ASSIGNMENTS` block). If `--dry-run`, stop here — do NOT apply the edit.

## Step 6 — Sanity Check

After the edit:

1. Run `python -m py_compile summons_etl_enhanced.py` — if this fails, the
   edit broke the file; STOP and report the compile error with the offending
   diff so the user can revert.
2. Import-check:
   `python -c "from summons_etl_enhanced import DFR_ASSIGNMENTS; print(len(DFR_ASSIGNMENTS))"`
   (run from the Summons root). Confirm the list length grew by 1.

## Step 7 — Optionally Run the ETL

If `--run-etl`:

```
python run_eticket_export.py
```

Then run the verification skill for the current report month (derive from
today's date as `YYYY-MM` of last completed month):

```
python scripts/verify_summons_against_raw.py --report-month YYYY-MM
```

Tell the user:
- Expected: `DFR_ASSIGNMENTS: applied window overrides to N ticket(s)` log
  line (N may be 0 if no tickets fall in the window yet — that's OK).
- Expected: `verify_summons_against_raw.py` exits 0.

## Step 8 — Summary

Print a final block:

```
## DFR Window Added

**Badge:**          {padded_badge}
**Officer:**        {standard_name}
**Assignment:**     {WG2}
**Window:**         {from_date} -> {to_date_or_"open-ended"}
**Reason:**         {reason}
**File edited:**    summons_etl_enhanced.py (line {N})
**Compile check:**  PASS
**ETL run:**        {ran | skipped}
**Verification:**   {exit 0 | skipped}
```

## Gotchas

1. **Badge must already exist in Personnel GOLD.** This skill does NOT hire
   officers. If HR told you someone transferred in and the badge isn't in
   the CSV yet, update `Assignment_Master_GOLD.xlsx` and run
   `python scripts/sync_assignment_master.py` in the Personnel repo first.
2. **Open-ended windows clip at runtime.** The ETL clips `date_end = None`
   to the data's max date via `normalize_date_windows`, so an open-ended TTD
   doesn't produce 70+ years of month-split rows. You don't need to close the
   window manually — when the officer comes off TTD, just edit `date_end`
   to the last TTD day and re-run.
3. **Overlap is a warning, not an error.** Later entries in
   `DFR_ASSIGNMENTS` win over earlier ones on overlap because the ETL loops
   through them in order and stamps each match. If you intentionally want a
   tight window inside a broader one, put the tighter window LAST.
4. **Don't commit from this skill.** Leave committing to the user — they
   usually want to bundle the DFR window change with other Summons edits.
5. **No side-effects on `ASSIGNMENT_OVERRIDES`.** This skill only touches
   `DFR_ASSIGNMENTS`. Badge-to-assignment static overrides (e.g. Ramirez's
   FIRE LANES conditional) live in a separate list and should be edited by
   hand when they change.
6. **Reason codes are not enforced.** The `reason` field is a free-form tag
   used in logs only. Keep it uppercase and underscore-separated for
   greppability.
7. **Dry-run first for high-profile officers.** For a brass or high-visibility
   assignment (anyone at Lt. or above, or any badge on the DFR active list),
   run `--dry-run` first and eyeball the diff before applying.

## Reference

- Target file: `02_ETL_Scripts/Summons/summons_etl_enhanced.py`
  - `DFR_ASSIGNMENTS` list: lines ~58–75 (verify at runtime — the list
    grows over time)
  - Helper that consumes it: `_apply_dfr_assignment_windows` around
    line 705
- Dependency: `scripts/summons_etl_normalize.py` → `normalize_date_windows`
- Personnel source of truth:
  `09_Reference/Personnel/Assignment_Master_V2.csv`
- Verification: `scripts/verify_summons_against_raw.py --report-month YYYY-MM`
- Related hardening memory:
  `docs/skill_memory/apply_dfr_assignment_windows_MEMORY.md`
