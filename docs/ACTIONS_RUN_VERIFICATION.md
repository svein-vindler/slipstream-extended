# Actions run receipts and offline daily reconciliation

The six existing scheduled data workflows use `pipeline.actions_receipt` around
their existing command entry points. The wrapper records execution evidence; it
does not schedule, retry, pause, restore or fetch data by itself. Existing cron,
concurrency, permissions, timeouts, source selection and data budgets remain in
the workflows and commands. The separate local daily archive operator is not
part of this feature.

## What a receipt establishes

The version 1 `actions-run-receipt` JSON contains only:

- Fixed schema/kind and one of the six workflow filenames.
- Validated run ID, attempt, head SHA, event and configured cron expression.
- The command's actual UTC start/end, original native exit and controlled
  command/data outcomes, with allowlisted nonnegative aggregate counters.
- Explicit context availability and the absence of unique trigger evidence.

It is printed as one JSON line in the existing job log, written to the workflow's
explicit runner-temporary receipt path, and summarized in `GITHUB_STEP_SUMMARY`.
There is no new artifact upload, service, secret, permission or R2 write. The
runner-temporary file is not durable storage; retain the existing log/summary
under the installation's normal retention policy when evidence is needed.

The shared executable schema is `pipeline/actions_receipt_schema.py`.
`scheduled_for`, `occurrence_id` and `job_conclusion` are always null in v1.
The command cannot observe the final job conclusion before the job ends. A later
offline audit uses separately supplied run/job observations for that conclusion.
`trigger_provenance` is `unavailable`: `github.event.schedule` supplies the cron
expression, not the identity of one intended occurrence.

For example, `native_exit: 0`, `command_outcome: success` and
`data_outcome: partial` are compatible and intentional. `complete` describes the
returned command report, not global freshness or completeness of all stored
fitness products. `no_work` requires explicit observed zero-work evidence;
absence of a counter is not zero. Persisted counters from a previous passive
queue run are not presented as this invocation's work. Coach and HRV per-run
counters require a report timestamp inside the actual invocation window;
unbound counters are omitted, without inventing observed zero. Unknown result versions
or statuses remain `unknown`. Known blocked, pending, unsupported and
waiting-for-profile results keep their distinctions.

The data command runs once with the same original parser arguments. The six
CLI functions additionally return their existing report to the wrapper; direct
CLI invocation and private persisted reports retain their existing behavior.
Each repository retains its own original calls, imports and provider context;
portable promotion does not add a different repository's data follow-up route.
Receipt, summary or cleanup failure does not replace an earlier primary exit
or repeat the command. A fixed availability notice identifies a receipt sink
failure without printing exception text or a private path.

The receipt starts at the data entry point. Checkout/setup/dependency failures,
shell validation before the entry, skipped jobs, forced process termination
and runner loss may produce no receipt. They require the separately observed
GitHub run/job facts; missing receipt is not evidence of successful execution.

## Fixed workflow mapping

| Workflow | Existing command | Cron (UTC, unchanged) |
| --- | --- | --- |
| `refresh.yml` | `pipeline.refresh --workflow` | `0 */6 * * *` |
| `activity-refresh.yml` | `pipeline.activity_refresh` | `15 23 * * *` |
| `scheduled-activity-backfill.yml` | `pipeline.activity_backfill_scheduler` | `30 2,8,14,20 * * *` |
| `scheduled-health-detail-backfill.yml` | `pipeline.health_detail_backfill` | `15 1,7,13,19 * * *` |
| `scheduled-hrv-backfill.yml` | `pipeline.hrv_backfill` | `30 4,10,16,22 * * *` |
| `coach-input-backfill.yml` | `pipeline.coach_backfill` | `40 2,8,14,20 * * *` |

The Coach `workflow_run` condition and all manual inputs are preserved. A Coach
run caused by another workflow is not a Coach cron occurrence. An additional
attempt of one run ID is not another scheduled occurrence.

## Offline reconciliation

Run only with explicitly prepared local evidence:

```console
python -B -X utf8 -m pipeline.actions_reconcile --input audit-input.json --output audit-report.json
```

The CLI has no network collector, GitHub client, credential loader, scheduler or
data repair action. It never retries or dispatches a data job. Inputs are read
only, and the report must not replace an input. There is no implicit search of
private runtime files, logs, R2 objects or the filesystem.

The version 1 manifest uses `kind: actions-reconcile-input` and these fields:

| Field | Meaning |
| --- | --- |
| `day_start`, `day_end` | Inclusive UTC audit dates, at most 31 days |
| `as_of` | Explicit UTC time of the comparison |
| `observation_start`, `observation_end` | The explicitly supplied observation coverage window |
| `delay_window_minutes` | Explicit tolerance, from 0 to 10080 minutes |
| `workflow_configs`, `runs`, `jobs`, `receipts`, `trigger_evidence`, `pauses` | Bounded lists of pinned JSON documents |

Every reference is exactly `{path, bytes, sha256}`. Relative paths resolve from
the manifest directory. The SHA is checked against bounded bytes before JSON is
used; it establishes byte identity, not source authenticity. The operator must
provide the genuine source observations and their intended observation window.
Offline verification cannot independently authenticate a GitHub response or
establish that pagination/collection was exhaustive.

The normalized documents are:

- Config: `workflow`, `schedules: [{cron_expression, timezone}]`. Supply the
  actual selected workflow configuration; timezone is an explicit IANA name.
- Run: `workflow`, `run_id`, `run_attempt`, `head_sha`, `event`, `status`,
  `conclusion`, `created_at`, `run_started_at`.
- Job: `workflow`, `run_id`, `run_attempt`, `conclusion`.
- Receipt: the unchanged version 1 receipt JSON described above.
- Pause: `workflow`, `paused_from`, `paused_until`,
  `provenance: operator-record`. A current disabled state alone does not prove a
  historical pause interval.
- Optional trigger evidence: the run/workflow/head/attempt identity,
  `event: schedule`, `scheduled_for`, `cron_expression`,
  `provenance: github-trigger-metadata`, `source_field: scheduled_for`, and a
  pinned `source_ref` containing the matching genuine trigger metadata. Neither
  a receipt nor a config can substitute for that source. The standard run
  metadata currently used by this project does not provide this field: leave
  the list empty rather than reconstructing it from run start or cron.

The report retains expectations, observed runs, unassigned/ambiguous evidence,
not-yet-due times, known pause windows and missing observations after the
explicit window. It does not claim that absence proves GitHub dropped a job.
Receipts finishing after `as_of` are incomplete evidence for that audit and
cannot establish an earlier complete/no-work result or its work counters.
An observation that starts after midnight may refer to an earlier occurrence
only when the supplied trigger evidence establishes that relationship. The
nearest cron time, run number and cron expression alone never establish it.

The configuration is an expectation model. UTC matching preserves timezone
offsets and separate candidates during repeated local hours. DST transition
ambiguity is explicit and makes the expectation set incomplete. In particular,
v1 does not calculate GitHub's advance-to-next-valid-time behavior for a skipped
local hour; it reports that interval as unresolved rather than inventing an
occurrence or claiming zero expected runs.

Limits: manifest 64 KiB; each referenced JSON at most 1 MiB; total read bytes at
most 8 MiB; at most 16 configs, 512 runs, 2048 jobs, 1024 receipts, 512 trigger
evidence documents, 128 pauses and 8192 generated expected times. These are audit bounds, not changes to any
data pipeline or cloud-account budget. Unsupported cron/timezone input remains
explicit instead of being silently interpreted with UTC or another expression.

## Synthetic example and verification

The portable fixture contains invented identifiers and future metadata times:

```console
python -B -X utf8 -m pipeline.actions_reconcile --input tests/fixtures/actions_run/input.json --output audit-example.json
```

It supplies a green job with a partial data receipt and no unique trigger proof.
The audit must preserve that partial result and leave the scheduled run
unassigned. It must not assign it to the closest one of four expected cron times.
The output path is explicit; do not point it at a source or input document.

Relevant regression tests exercise the actual receipt entry and existing six
parsers with synthetic data-command boundaries, actual offline CLI/pinned JSON,
and the six actual YAML fragments. They cover primary nonzero exits, sink
failures, redaction, partial/no-work/not-ready/unknown distinctions, missing
context, timestamps/cross-midnight delays, retries, non-cron events, pins and
resource bounds. They make no live Garmin/R2/GitHub requests and cannot prove
production delivery or historical unique trigger coverage.

Rollback is to restore the reviewed workflow command lines and remove the new
receipt/audit entry modules and documentation as one source change. No stored
data schema, historical analysis, credential, deployment, schedule or security
configuration needs a migration or data rollback.

## Primary references

- [GitHub schedule events and timezone behavior](https://docs.github.com/en/actions/reference/workflows-and-actions/events-that-trigger-workflows#schedule)
- [GitHub workflow run metadata](https://docs.github.com/en/rest/actions/workflow-runs#get-a-workflow-run)
