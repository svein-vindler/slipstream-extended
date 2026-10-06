# Private backup and isolated local recovery

This first delivery is an optional local tool with four operations: `create`,
`verify`, `plan`, and `restore-local`. It does not contact Garmin, write R2,
register MCP tools, deploy a Worker, or schedule work. Production restore is
unsupported. The existing sync flow, measurements, hosted runtime dependencies,
budgets and authentication contracts are unchanged.

## Exact coverage

The source adapter enumerates only these canonical v1 namespaces:

| Namespace | Coverage |
| --- | --- |
| `coach/profiles/v1/YYYY-MM-DD/<profile-id>.json` | Every profile version in this namespace, including zones, threshold/reference values and effective dates |
| `activities/<year>/<id>/context/v1/<timestamp>-<context-id>.json` | Every append-only context version for each explicitly selected activity: RPE, conditions, note and workout correction |
| `activities/<year>/<id>/coach-input/v1/canonical/<analysis-id>.json` | Every immutable historical analysis for each selected activity, with original stored bytes |

The encrypted manifest records scope, canonical keys, type, source revision,
stored/decoded sizes, SHA-256 checksums, and explicit analysis-to-profile/context
links. Embedded profile and context versions must agree with the corresponding
canonical objects. Unknown schemas, unexpected fields/types, missing references
and conflicting versions fail the entire operation; they are never skipped.
An activity may have context without a coach analysis. Profiles may be backed up
without selecting any activities.

**This is complete only for the declared scope, not for the installation or the
whole bucket.** Supply every relevant activity explicitly when seeking full
user-context/analysis coverage; the tool cannot discover omitted activities.
If a scope exceeds the limits, stop and review a smaller explicit selection.
Do not raise limits or initiate extensive backfill automatically.

Excluded: Garmin sessions/tokens, GitHub/Cloudflare credentials, environment and
login files, raw FIT/TCX, GPS, activity/health CSV summaries, raw/normalized Garmin
objects, source manifests, bucket-wide inventories, backfill plans, leases,
refresh reports, profile read indexes and `latest-ready.json` pointers.

Coach inputs contain private **derived fitness data**, including historical
summaries, workout steps, laps, splits and source hashes. They are encrypted but
are not a backup of their source telemetry. Free-text notes are preserved as
data; never put credentials into them. Schema checks cannot identify every
secret or coordinate somebody might type into an allowed text field.

## Format and password handling

Format v1: fixed `SLIPSTREAM-BACKUP-1` header, 16-byte random salt, and one standard
Fernet token containing the versioned JSON manifest and base64 object bytes.
Manifest, object names, scope and references are all inside the authenticated
ciphertext. Only format, salt, file length and Fernet's creation timestamp are
visible. JSON serialization/base64 are containers, not a custom cipher.

The maintained `cryptography` library supplies Fernet and Argon2id; parameters
are fixed by v1: 32-byte derived key, three iterations, four lanes and 64 MiB
Argon2 memory. See the library's [Fernet password recipe and memory limitation](https://cryptography.io/en/stable/fernet/#using-passwords-with-fernet).
Library authentication completes before any manifest parsing or unpacking.
Canonical base64 is required; trailing garbage, tampering, truncation and wrong
passwords are rejected. SHA-256 additionally verifies each authenticated object.

Passwords are entered only through a hidden interactive prompt, repeated during
creation. There is no password/key command argument, environment variable or
password file option. Unavailable hidden prompting fails closed. Passwords must
be 12–1024 UTF-8 bytes; use a strong unique password kept separately in a password
manager. Lost passwords cannot be recovered by Slipstream. Python cannot
guarantee erasure of password or plaintext copies from process memory; use a
trusted computer with encrypted storage and an appropriate swap/dump policy.

No persistent plaintext scratch files are created during create/verify/plan.
Creation writes a randomly named **ciphertext-only** partial file, flushes it,
then publishes using an exclusive same-filesystem hard link. A partial file is
never the requested completed backup. Ordinary failures clean up that partial;
a killed process can leave a `.slbk-part-*` file. Filesystems without hard-link
support fail publication. Existing backups are never replaced, including races.

## Local use: Windows

Use Python 3.12 or newer. In the repository root:

```powershell
py -3.12 -m venv .venv
.venv/Scripts/python.exe -m pip install --require-hashes -r requirements-backup.txt
```

Choose an existing **private parent directory outside Git and shared folders**.
Backup creation, recovery planning and local restore reject destinations inside
a Git checkout, including worktrees with a `.git` file. Input archives and source
objects must be regular files; directory entries are enumerated incrementally
under the fixed directory and object limits.
Use Windows Security/Advanced permissions to restrict that parent to your user
and required system administrators, and use BitLocker/device encryption where
appropriate. The tool rejects symlinks and junctions; it cannot certify your
Windows account, parent ACLs, disk encryption or another process's access.

The local source tree must already contain approved offline objects or synthetic
fixtures, laid out exactly like the namespaces above. No source copy or download
is initiated by the tool. In the private parent create `scope.json`, for example
(invented identifier):

```json
[{"year":"2026","activity_id":"900001"}]
```

With private paths chosen by you:

```powershell
.venv/Scripts/python.exe -m pipeline.private_backup create --source C:/Private/source --scope C:/Private/scope.json --output C:/Private/history.slbk
.venv/Scripts/python.exe -m pipeline.private_backup verify --backup C:/Private/history.slbk
.venv/Scripts/python.exe -m pipeline.private_backup plan --backup C:/Private/history.slbk --target C:/Private/recovery-test-001
.venv/Scripts/python.exe -m pipeline.private_backup restore-local --backup C:/Private/history.slbk --target C:/Private/recovery-test-001
```

These are examples, not commands to run against a live installation. Output is
counts, byte totals, operation counts and fixed status/error codes. It never
prints passwords, keys, activity identifiers, notes or provider error messages.
Planning reads and validates the backup and checks target absence; it writes
nothing. Exit status is 0 on success and 2 on refusal/failure.

## Local use: Linux

```bash
python3 -m venv .venv
.venv/bin/python -m pip install --require-hashes -r requirements-backup.txt
umask 077
mkdir -m 700 "$HOME/slipstream-private"
# Prepare scope.json and a synthetic/approved offline source in this private area.
.venv/bin/python -m pipeline.private_backup create --source "$HOME/slipstream-private/source" --scope "$HOME/slipstream-private/scope.json" --output "$HOME/slipstream-private/history.slbk"
.venv/bin/python -m pipeline.private_backup verify --backup "$HOME/slipstream-private/history.slbk"
.venv/bin/python -m pipeline.private_backup plan --backup "$HOME/slipstream-private/history.slbk" --target "$HOME/slipstream-private/recovery-test-001"
.venv/bin/python -m pipeline.private_backup restore-local --backup "$HOME/slipstream-private/history.slbk" --target "$HOME/slipstream-private/recovery-test-001"
```

Use a new recovery directory each time; it must not exist. All content is
validated before the directory is created. The directory is mode 0700 (Windows
uses Python's corresponding private-directory creation behavior). Plaintext is
written only into the explicitly chosen area: original objects under `objects/`,
and `manifest.private.json` containing private metadata/reference details.
`RESTORE-INCOMPLETE` remains after an interrupted restore; `RESTORE-COMPLETE`
means the isolated historical copy was written and checksummed, not that an
installation has been recovered. Never share this directory or add it to Git.

## Derived pointers and source consistency

Profile indexes and analysis readiness pointers are omitted. Every restored
analysis reference is marked `ready: false` with
`external_sources_not_backed_up`. FIT/TCX hashes and missing source-object names
are retained privately as provenance, not treated as evidence of availability.
Historical analyses remain byte-identical even if present-day sources changed.
No analysis is advertised as current/ready in this delivery, including when an
external source might happen to exist. A future production migration must verify
every required canonical source and its version before rebuilding derived state.
The local output is deliberately not a ready-to-upload installation snapshot.

Export compares a bounded inventory before and after reading all selected
objects. The R2 adapter uses listing ETags and conditional GET (`IfMatch`) and
checks response revision/length. Local sources hash their file contents at both
inventory passes and at read time. Missing/changed/new objects abort publication.
There is no automatic retry by default; `create --retries 1` permits one complete
restart. Exhaustion reports `source_not_consistent` and produces no final file.

This is revision-checked scoped export, not an atomic bucket snapshot. It cannot
detect an add/delete or change-and-revert entirely between observations. For a
future approved live test choose a quiet source interval, review the relevant
writers and scope, and do not change sync schedules while measuring the baseline.

## Fixed resource limits

| Resource | Maximum |
| --- | --- |
| Explicit activities | 100 |
| Canonical objects, including all profiles | 1,000 |
| Stored bytes per object / aggregate | 1 MiB / 8 MiB |
| Decoded JSON per object / aggregate | 2 MiB / 16 MiB |
| Encrypted document before Fernet / archive file | 12 MiB / 17 MiB |
| JSON nesting / structural node budget | 32 levels / 100,000 across decoded objects (also bounded before JSON parsing) |
| Source SDK calls, including all attempts | 10,000 logical LIST/GET calls |
| Complete export restarts | 0 by default, at most 1 |

Limits may be lowered in the library API, never increased past these maxima.
Stored bytes, base64 lengths, decoded lengths, JSON structure and aggregate
totals are checked before further allocation wherever possible. Gzip reads stop
at the decoded limit; there is no general tar/zip extraction. Fernet needs the
whole bounded message in memory. KDF allocation is fixed at 64 MiB and data
buffers/node counts are bounded; process RSS includes interpreter/native-library
overhead and is not an OS-enforced memory quota. This format is intentionally
unsuitable for a whole-bucket or large-file backup.

For R2, a single stable attempt uses `N` GETs plus two passes over
`1 + 2 × activity_count` exact prefixes (more LIST pages only when paginated).
At most 4,000 listed records are accepted over all attempts. Adapter operation
counters are cumulative, including aborted attempts. SDK retries must be limited
to one, so network attempts can be up to twice logical operation counts. R2 PUTs,
Garmin calls, Worker requests and Actions runs are zero. Local-source inventory
hashing adds local file reads, included in its counters and read-byte total.
There is no free-operation guarantee; assess future live reads against aggregate
account usage and current provider limits before authorization.

## Validation and keeping a separate private copy

Development requirements include the backup dependency so ordinary Python CI
does not silently skip its synthetic tests. Hosted sync's `requirements.txt` and
all workflows stay unchanged. Reproduce locally:

```bash
python -m pip install --require-hashes -r requirements-dev.txt
python -m pytest -q tests/test_private_backup.py
ruff check .
python -m pip_audit -r requirements-backup.txt --disable-pip
```

Regenerate the optional lock with `pip-compile --generate-hashes
--strip-extras --no-emit-index-url requirements-backup.in`, then regenerate the
development lock according to CONTRIBUTING.md. Keep existing dependency pins
and Windows markers intact.

After an approved real export, verify it, then copy only the `.slbk` ciphertext
to a separate private location such as encrypted removable storage. Verify the
second copy independently. Keep its password separately. Do not place backups,
scope files, recovery directories or plaintext sources in Git, public cloud
folders, Actions artifacts, PR attachments or MCP. `.gitignore` blocks standard
backup/partial extensions and `.private-backup/`; it cannot protect arbitrary
names or override `git add -f`. This delivery sets no retention policy or
automatic copy/removal task. Manual retention choices remain with the owner.

## Manually approved live pilot

The optional `pipeline.backup_pilot` helper now wires the read-only adapter into
a separate local pilot. Use it only after approval for the selected activity,
private directory and budget. It neither changes the offline backup CLI nor
adds a hosted job. The owner must supply an R2 token with **Object Read only**
permission scoped to the correct bucket; consult [Cloudflare's token documentation](https://developers.cloudflare.com/r2/api/tokens/).
No write probe or token-management API is used to infer permissions. The helper
cannot itself certify the permissions of the supplied key.

Use the repository's runtime and backup dependencies in the same local environment:

```powershell
.venv/Scripts/python.exe -m pip install --require-hashes -r requirements.txt -r requirements-backup.txt
.venv/Scripts/python.exe -m pipeline.backup_pilot --private-root C:/Private/Slipstream
```

Linux uses `.venv/bin/python` with the same module and an existing private root.
The root must be outside Git and must already exist with private permissions.
The activity year/ID, credential permission check, Cloudflare account, bucket,
S3 access/secret key and backup password are entered in hidden local prompts.
An optional `--scope` file can preselect the single activity without putting its
identifier in command arguments. Keep that small selection file in the chosen
private area, outside Git; it contains identifiers but never credentials.
There is no credential argument, file, environment discovery or network call
before those inputs are supplied. The S3 endpoint is derived only from a
validated account ID. [SDK configuration](https://docs.aws.amazon.com/botocore/latest/reference/config.html)
fixes connection/read timeouts at 10/30 seconds and total attempts at one.

Pilot caps are lower than the general format: **one activity, all profile
versions, 100 objects, 1 MiB stored total, 2 MiB decoded total and 20 logical
LIST/GET calls**. No SDK or export retries are allowed. With one page per prefix,
six LIST calls leave room for at most 14 GETs; the object-count cap is an
additional guard, not a promise to fetch 100 objects within 20 calls. If source
history exceeds any cap, the pilot fails without raising it automatically.

A newly created private session contains the encrypted backup, byte-identical
verified ciphertext copy, isolated plaintext recovery directory and an anonymous
counts-only result. The extra ciphertext copy is in the same private location;
it does not constitute an off-device backup. `PILOT-INCOMPLETE` remains on failure.
Success requires verified bytes/references, at least one covered object for the
chosen activity, and `PILOT-COMPLETE` without `PILOT-INCOMPLETE`. A profiles-only
export never claims successful activity coverage. No analysis is marked ready.
Provider errors and SDK logging are suppressed in this separate local process;
only fixed error codes and operation/byte totals are printed.

## Local verification and remaining approval boundaries

Local delivery validation on Windows with Python 3.14: 633 Python tests passed
(79 backup tests and ten pilot tests), 102 Worker unit tests and 75 runtime tests passed. Ruff,
Worker typecheck, hash-locked installation, `pip check`, public release privacy
check, Python runtime/backup vulnerability audits and Worker runtime dependency
audit passed. Windows junction rejection was exercised when symlink creation
was unavailable. Linux commands are documented; a Linux runtime was not used
for this local verification. A separately authorized, bounded read-only R2
pilot also passed authenticated verification, preview without writes, isolated
local restore with identical bytes/references, and independent verification of
the ciphertext copy. Production restore was not tested. Private selection,
credentials, backup contents and installation values remain outside Git.

The offline backup CLI has no live switch. Synthetic adapters exercise
`R2ReadOnlySource` and the separate pilot helper, including failure paths.
Before every additional live test, obtain approval for exact activity scope,
private output location, read operation budget and a read-only R2 credential;
keep the pilot's finite network timeouts and zero retries. Keep secrets out of
command arguments and logs.
Then perform scoped export, verification and isolated local restore, compare
bytes/references privately, and retain only anonymous totals in the report.
Production restore, schedules, deployment and public promotion remain separate
approvals. Publishing and merging code requires explicit authorization and
successful CI. Public promotion must carry only reviewed portable source,
tests and documentation with public noreply authorship, never private history.
