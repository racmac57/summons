# How to Use `/add-ttd-window`

A friendly guide for telling the Summons ETL "this officer is on a special
assignment for this date range only." No code editing required — Claude does
the careful part for you.

---

## What this is for

Sometimes an officer's assignment changes for a stretch of days or months and
you need the DFR (Drone / Directed Patrol Enforcement) report to show that
correctly — without retroactively rewriting their old tickets.

Real-life examples:

- **Officer Smith twisted his knee on Monday and is on light duty for the next
  three weeks.** You want every ticket Smith writes between Monday and the end
  of the month to show `LIGHT_DUTY` in the DFR sheet, but the tickets he wrote
  the week *before* his injury should stay as his normal patrol assignment.
- **Lt. Dominguez started transitional duty (TTD) on April 6 and we don't
  know yet when he's coming back.** All of his tickets from April 6 onward
  should show `TTD`, indefinitely, until you tell the system otherwise.
- **Ramirez was loaned to the SSOCC detail for the first four days of March.**
  Tickets dated March 1–4 should show `SSOCC`; March 5 onward goes back to his
  baseline.

These can't be handled by the regular `ASSIGNMENT_OVERRIDES` lookup because
that one has no concept of dates — it would stamp **every** ticket the
officer ever wrote with the new assignment, including historical ones.
`DFR_ASSIGNMENTS` (the date-windowed list) is what we use instead, and
`/add-ttd-window` is the safe way to add a row to it.

---

## Before you start

You need three things:

1. **The badge number.** Any length — single-digit legacy badges like `#1` or
   `#15` work fine, the skill pads them to 4 digits internally.
2. **The dates.** Inclusive `YYYY-MM-DD`. If you don't know when the
   assignment ends yet, use `--open-ended`.
3. **The assignment code.** Whatever you want to land in the `WG2` column —
   typically `TTD`, `SSOCC`, `LIGHT_DUTY`, `ADMIN_LEAVE`. Uppercase, no
   spaces.

The officer must already exist in `Assignment_Master_GOLD.xlsx`. If they're a
brand-new hire who isn't in Personnel yet, add them to GOLD and run
`python scripts/sync_assignment_master.py` in the Personnel repo first. The
skill will refuse to add a window for a badge it can't find — by design.

---

## How to run it

In Claude Code, from the Summons repo, type:

```
/add-ttd-window --badge 0421 --assignment LIGHT_DUTY --from 2026-05-04 --to 2026-05-25 --reason LIGHT_DUTY
```

That's the Officer Smith example: badge 421, light duty, May 4 through
May 25 inclusive.

For Lt. Dominguez (open-ended):

```
/add-ttd-window --badge 0115 --assignment TTD --from 2026-04-06 --open-ended --reason TRANSITIONAL_DUTY
```

For Ramirez's SSOCC detail (already in the file as the historical example):

```
/add-ttd-window --badge 2025 --assignment SSOCC --from 2026-03-01 --to 2026-03-04 --reason MARCH_WINDOW
```

### Useful flags

- `--dry-run` — show me the diff but don't actually save it. Always do this
  first if you're nervous, especially for a Lt. or above.
- `--run-etl` — after saving, automatically re-run `run_eticket_export.py`
  and the verification script so you can see the new assignment land in the
  staging file in one shot.

---

## Why the dates "just work" across month boundaries

This is the part that usually trips people up with date-range overrides, so
here's the plain-English version of the guardrail.

**The problem we're avoiding.** Lt. Dominguez goes on TTD on April 6 with no
end date. If we naively said "from 2026-04-06 to forever," the ETL would try
to apply that to a date range covering 70+ years of split month-segments —
slow, ugly, and a memory hog. And if we said "April 6 to April 30," that
would silently stop applying when May arrives.

**What `/add-ttd-window` actually does.** When you use `--open-ended`, the
ETL clips the end date at runtime to whatever the latest ticket date is in
the data — typically last week or last month. So today's run might generate
"April 6 to April 14" as one month-segment. Next month's run will
auto-extend it to "April 6 to May 31" → "April 6 to April 30" + "May 1 to
May 31" — *two* month-segments, one per calendar month.

**Why one segment per calendar month?** Because every downstream Power BI
slicer and DAX measure in this stack is bucketed by month. A window that
crosses March/April gets split into a March segment and an April segment, so
"how many tickets did Dominguez write under TTD in March?" and "...in April?"
both come out cleanly without needing extra date math in the report.

You don't have to think about any of this — `normalize_date_windows` in
`scripts/summons_etl_normalize.py` handles it on every run. You just give
the skill the start date and either the end date or `--open-ended`, and the
guardrail takes care of the rest.

---

## When the officer comes off the special assignment

Just open `summons_etl_enhanced.py`, find the entry in the `DFR_ASSIGNMENTS`
list, and change `"date_end": None` to `"date_end": "YYYY-MM-DD"` — the last
day they were on the special assignment. (Or rerun `/add-ttd-window` and let
the skill handle the edit if you'd rather not touch the file.)

The next ETL run will start treating tickets after that date as the
officer's normal assignment again.

---

## Sanity checks the skill runs for you

After it edits the file, the skill automatically:

1. Runs `python -m py_compile summons_etl_enhanced.py` to make sure the file
   still parses. If it broke, the skill stops and tells you what went wrong
   so you can revert.
2. Re-imports the file and confirms `DFR_ASSIGNMENTS` grew by exactly one
   entry.
3. (If you passed `--run-etl`) runs the full ETL plus
   `python scripts/verify_summons_against_raw.py --report-month YYYY-MM` and
   reports the exit code.

If anything fails along the way, you get a clear error message and the file
is either left untouched (Step 1 / 2) or left in a known state (Step 3 — the
ETL doesn't touch the source code).

---

## What the skill won't do

- **It won't commit your change.** You'll see the diff and the success
  message, but staging and committing is up to you — usually you want to
  bundle a DFR window edit with other Summons changes.
- **It won't push.** Same reason.
- **It won't add an officer to Personnel.** That's
  `python scripts/sync_assignment_master.py` in the Personnel repo, after
  you edit GOLD.
- **It won't enforce a list of allowed reason codes.** The `--reason` field
  is a free-form tag for log greppability. Keep it uppercase and
  underscore-separated (`LIGHT_DUTY`, `MILITARY_LEAVE`, `ADMIN_LEAVE`).

---

## Quick reference

```
# Three-week light duty
/add-ttd-window --badge <NNN> --assignment LIGHT_DUTY \
                --from YYYY-MM-DD --to YYYY-MM-DD \
                --reason LIGHT_DUTY

# Open-ended TTD
/add-ttd-window --badge <NNN> --assignment TTD \
                --from YYYY-MM-DD --open-ended \
                --reason TRANSITIONAL_DUTY

# Preview only (recommended for high-profile officers)
/add-ttd-window --badge <NNN> --assignment <CODE> \
                --from YYYY-MM-DD --to YYYY-MM-DD \
                --reason <CODE> --dry-run

# Edit + run the full ETL + verify
/add-ttd-window --badge <NNN> --assignment <CODE> \
                --from YYYY-MM-DD --to YYYY-MM-DD \
                --reason <CODE> --run-etl
```

---

## Where to look next

- Skill spec: `.claude/skills/add-ttd-window/SKILL.md`
- The list this writes to: `summons_etl_enhanced.py` →
  `DFR_ASSIGNMENTS` (around line 58)
- Date-window splitter: `scripts/summons_etl_normalize.py`
- Reconciliation: `scripts/verify_summons_against_raw.py --report-month YYYY-MM`
- Hardening notes:
  `docs/skill_memory/apply_dfr_assignment_windows_MEMORY.md`,
  `docs/skill_memory/add_ttd_window_MEMORY.md`
