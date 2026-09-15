# Instructor notes — Bronze ingestion lab

**Not a student document.** Students follow `bronze-ingestion.md` only.

The lab runs the pipeline three times to show incremental loading. That only
works if the last three months are missing from the repository when the class
starts, and reappear part-way through.

`nb_list_event_files` lists files through the **GitHub Contents API**, so both
actions below must be **committed and pushed** — moving files in a local clone
has no effect on students.

## Action 1 — before the session: park the last three months

```powershell
cd <path-to-repo>\data
Move-Item events\workforce_events_2025-1*.csv _parked_events\
git add -A
git commit -m "Park 2025-10..12 for the incremental lab"
git push
```

`data/events` now holds 57 files, ending at `workforce_events_2025-09.csv`.

## Action 2 — after everyone's initial run: bring them back

Run this once the whole room has finished the **initial run** (step 6.1) and
before anyone starts the **incremental run** (step 6.2).

```powershell
cd <path-to-repo>\data
Move-Item _parked_events\workforce_events_2025-1*.csv events\
git add -A
git commit -m "Release the final three months"
git push
```

All 60 files are published again, so the incremental run copies exactly three
files and the no-op run finds nothing new.

## Notes

- `2025-1*` matches only `2025-10`, `2025-11`, and `2025-12`; `2025-01` through
  `2025-09` contain a `0` and stay in place.
- Leave the repository restored after class (60 files in `data/events`,
  `data/_parked_events` empty) so the next session can start from action 1.
