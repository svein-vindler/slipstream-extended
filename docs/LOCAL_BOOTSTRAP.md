# Optional local overnight bootstrap

The normal GitHub workflows are the default and require no computer to remain
online. For a new installation with several years of history, the same
resumable jobs can optionally run on a trusted local computer for a few hours.

This is an accelerator, not a separate importer. By default it calls the
existing activity, HRV, sleep and body-composition backfills, writes the same
canonical R2 objects, observes the same limits, and resumes from the same R2
progress plans. Coach generation is a separate R2-only consumer so it does not
repeatedly scan activity artifacts inside the Garmin loop. The process can be
interrupted at any time and later restarted.

Activity discovery already uses Garmin's date-range endpoint. Body-composition
backfill groups missing dates into bounded date windows and uses Garmin's range
endpoint once per window. HRV and sleep are exposed by Garmin as daily calls;
the local runner still accepts the whole history as one job, reuses one login,
paces those daily requests, and uploads each normalized day as it completes.

The computer's CPU, GPU and internet connection are rarely the limiting factor.
Garmin request tolerance and R2 safety limits determine the pace. Do not increase
the per-cycle batch limits beyond the documented maximums.

## 1. Prepare local secrets

Install the Python dependencies in the normal project environment. Copy
`.env.local-bootstrap.example` to `.env` and fill in the three R2 credentials.
The `.env` file and Garmin token files are ignored by Git.

GitHub secrets cannot be downloaded again. If the Garmin token only exists in
GitHub, mint a fresh local session token:

```bash
python scripts/garmin_login.py \
  --local-token-file garmin_tokens.local.txt \
  --local-only
```

This asks for the Garmin password and MFA code interactively, then stores only
the refreshable session token in the ignored local file. Never commit or share
`.env` or the token file.

## 2. Pause cloud jobs

Local and GitHub Garmin jobs must not overlap. Disable these scheduled workflows
before starting the overnight process:

```bash
gh workflow disable refresh.yml
gh workflow disable scheduled-activity-backfill.yml
gh workflow disable scheduled-hrv-backfill.yml
gh workflow disable scheduled-health-detail-backfill.yml
gh workflow disable activity-refresh.yml
gh workflow disable coach-input-backfill.yml
```

Check the Actions page (or `gh run list`) and wait for any already-running Garmin
job to finish. Manual pilot/backfill jobs must not be started while local mode is
active.

## 3. Run overnight

From the repository root:

```bash
python -m pipeline.local_bootstrap \
  --confirm-cloud-jobs-paused \
  --max-hours 8
```

On Windows PowerShell with the repository virtual environment:

```powershell
.\.venv\Scripts\python.exe -m pipeline.local_bootstrap `
  --confirm-cloud-jobs-paused `
  --max-hours 8
```

Defaults process at most 50 activities, 100 HRV dates, 100 sleep dates and 100
body dates per cycle, followed by a three-minute pause. Every phase gets a fresh
instance of the existing per-run R2 guard, while global storage and object
ceilings are checked before its first write. The status file records elapsed
time per phase so bottlenecks are visible. Garmin authentication, rate-limit,
service or R2-budget errors stop the process instead of retrying aggressively.

Useful options:

```text
--max-cycles 1                 Run one bounded cycle as a smoke test
--phases activity,health       Run only selected Garmin phases
--phases coach                 Run the separate R2-only coach consumer
--pause-seconds 600            Use a longer pause between cycles
--status-file <path>           Give a parallel R2-only consumer its own status
--lock-file <path>             Give a parallel R2-only consumer its own lock
--retry-failures               Explicitly retry items paused after three failures
```

Only use `--retry-failures` after investigating the recorded failures. Progress
is written to `.granular/local-bootstrap/status.json`. A local process lock
prevents two copies from running at once. `Ctrl+C`, shutdown or a network failure
does not invalidate completed R2 objects; rerun the same command to resume.

The dedicated coach workflow is triggered after successful activity-producing
workflows and also runs on its own schedule. Its persisted per-activity source
index means an unchanged activity is not downloaded from R2 again. To catch up
locally after the Garmin phases are finished, run:

```powershell
.\.venv\Scripts\python.exe -m pipeline.local_bootstrap `
  --confirm-cloud-jobs-paused `
  --phases coach `
  --max-hours 8 `
  --pause-seconds 30
```

## 4. Restore normal schedules

After completion or interruption, re-enable every disabled workflow:

```bash
gh workflow enable refresh.yml
gh workflow enable scheduled-activity-backfill.yml
gh workflow enable scheduled-hrv-backfill.yml
gh workflow enable scheduled-health-detail-backfill.yml
gh workflow enable activity-refresh.yml
gh workflow enable coach-input-backfill.yml
```

The local status `complete` means all selected plans are complete or contain
only deliberately blocked items. The scheduled jobs may then remain enabled;
their normal self-quiescing checks notice future data without rebuilding history.

For activity backfill, an empty but valid Garmin TCX file is finalized with an
explicit unavailable endurance object instead of remaining blocked. If a real
replacement TCX is supplied for a failed export, rerun the affected range with
`--retry-failures`; the activity plan will recheck its saved ranges and return
to `complete` once every canonical artifact exists.

## Import an existing raw Garmin export

Users who already have raw Garmin HRV or sleep JSON can seed the same canonical
R2 objects without calling Garmin again. Use raw exports, not flattened CSV or
`cgpt_*` files. The importer validates dates, normalizes with the live pipeline,
compresses each day, skips existing objects and shares the local-process lock so
it cannot overlap the normal local bootstrap.

Preview without writing:

```powershell
.\.venv\Scripts\python.exe -m pipeline.local_import `
  --confirm-cloud-jobs-paused `
  --hrv-file "D:\exports\hrv.json" `
  --sleep-file "D:\exports\sleep.json" `
  --dry-run
```

Import and resume automatically until the files are exhausted:

```powershell
.\.venv\Scripts\python.exe -m pipeline.local_import `
  --confirm-cloud-jobs-paused `
  --hrv-file "D:\exports\hrv.json" `
  --sleep-file "D:\exports\sleep.json" `
  --max-hours 8
```

Each cycle writes at most 200 missing objects by default, below the same
per-process R2 safety ceiling used elsewhere. Progress is written to
`.granular/local-import/status.json`; rerunning the command safely resumes by
checking canonical object keys in R2. Combined exports using `metric` and `data`
wrappers are also accepted. Source files and their absolute paths are never
uploaded or committed.
