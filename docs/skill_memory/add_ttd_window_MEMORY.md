# add-ttd-window (SKILL.md) — Hardening Memory

**Path:** `.claude/skills/add-ttd-window/SKILL.md` (Summons repo)
**Type:** prompt-file skill (static analysis — no executable)
**Status:** PASS (9/9) — 2026-04-14

## Binary Scorecard

| # | Test | Result | Evidence |
|---|------|--------|----------|
| 1 | Exists & Loadable | 1 | File present; YAML frontmatter parses via `yaml.safe_load`; keys `['allowed-tools','argument-hint','description','disable-model-invocation','name']` all populated |
| 2 | Shared Context Access | 1 | All 6 referenced paths resolve: `summons_etl_enhanced.py`, `run_eticket_export.py`, `scripts/summons_etl_normalize.py`, `scripts/verify_summons_against_raw.py`, `Assignment_Master_V2.csv`, `docs/skill_memory/apply_dfr_assignment_windows_MEMORY.md` |
| 3 | Path Safety | 1 | One absolute path (`Assignment_Master_V2.csv`) uses canonical `C:\Users\carucci_r\OneDrive - City of Hackensack` — matches global CLAUDE.md. All other refs are project-relative. No `~/`, no `RobertCarucci` |
| 4 | Data Dictionary Compliance | 1 | References canonical names: `DFR_ASSIGNMENTS`, `PADDED_BADGE_NUMBER`, `STANDARD_NAME`, `WG2`, `OFFICER_DISPLAY_NAME`, `date_start`, `date_end`, `reason` — match the dict keys actually in `summons_etl_enhanced.py:58-75` |
| 5 | Idempotency / Safe Re-run | 1 | `--dry-run` flag documented for preview; Step 6 confirms list length +1 after edit; re-running with the same args would insert a duplicate — Gotcha #3 calls out overlap warning as the guardrail |
| 6 | Error Handling | 1 | Step 1 enumerates abort conditions (missing flag, mutex violation, bad date, end<start, badge not numeric or >4 digits). Step 2 aborts on unknown badge with actionable message. Step 6 aborts on `py_compile` failure |
| 7 | Output Correctness | 1 | Step 8 summary block enumerates every field (badge, officer, assignment, window, reason, file/line, compile, etl, verify). Step 5 shows a unified diff before edit |
| 8 | CLAUDE.md Rule Compliance | 1 | Gotcha #1 routes new-hire flow through Personnel GOLD → sync script (Personnel Rule 1/2). Gotcha #4 forbids auto-commit. Badge validation respects new `feedback_badge_length_validation.md` memory (legacy #1/#15 badges allowed) |
| 9 | Integration / Cross-Skill Safety | 1 | Composes cleanly with `_apply_dfr_assignment_windows` (writes to the same list the helper consumes). Gotcha #5 explicitly isolates from `ASSIGNMENT_OVERRIDES`. Chains to `verify_summons_against_raw.py` for post-run validation |

**Score:** 9/9 PASS

## Iteration History

- Initial draft required badges to be 3–4 digits. User correction flagged HPD legacy 1- and 2-digit badges (`#1`, `#15`, `#27`). Relaxed to "numeric, ≤4 digits" across frontmatter, Arguments section, and Step 1. Captured in `feedback_badge_length_validation.md` (global memory) so future skills don't repeat the rule.

## Regression Guards

- **Frontmatter YAML parses cleanly.** If any future edit adds unquoted special chars to `description` or `argument-hint`, `yaml.safe_load` would fail this test — re-validate before committing.
- **Line number refs (`lines ~58–75`, `line 705`) are marked "verify at runtime"** in the Reference section so they don't become load-bearing lies as the file grows.

## Known Gotcha

- The Step 4 dict template uses curly-brace placeholders (`"{padded_badge}"`, `{to_date_or_none}`) that look like Python f-strings. The executing agent must substitute literally — NOT pass them to Python formatting, or a `None` literal becomes the string `"None"`. Line 105–106 call this out explicitly.
- Step 5's Edit anchor ("closing bracket is unique enough") depends on the `]` of `DFR_ASSIGNMENTS` being followed by a blank line and the `# ─── DFR Configuration` comment. Holds today. Re-check if anyone reorders module-level constants.
