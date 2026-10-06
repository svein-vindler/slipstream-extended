# Backup quick start

Use this guide on your own Windows or Linux computer. You need Python 3.12 or
newer and a normal copy of the published source. Download **Code → Download ZIP**
from [Slipstream Extended](https://github.com/svein-vindler/slipstream-extended)
and extract it into a new software folder, or clone the public repository.
Open a terminal in that source folder; it contains `backup.py` and
`requirements-backup.txt`. No Codex workspace is required.

## Install once

Create a separate backup environment in the fresh source folder. If
`.venv-backup` already exists, check and reuse that installation rather than
recreating it. The hash-locked backup dependencies are the only packages
required for these offline commands; no Worker, Node or Cloudflare setup is
needed. Installing them downloads software and does not read Garmin or R2.

Windows PowerShell:

```powershell
py -3 -m venv .venv-backup
.venv-backup/Scripts/python.exe -m pip install --require-hashes -r requirements-backup.txt
.venv-backup/Scripts/python.exe backup.py --help
```

Linux:

```bash
python3 -m venv .venv-backup
.venv-backup/bin/python -m pip install --require-hashes -r requirements-backup.txt
.venv-backup/bin/python backup.py --help
```

The help lists `create`, `verify`, `plan` and `restore-local`. On Linux, your
distribution may require its Python venv package before environment creation.
An installation error is a reason to stop; do not run the main service's setup
or deployment scripts to install this offline tool.

## Choose your private data directory

Keep backups, selection files, offline source objects and recovery folders in
an explicitly chosen private area outside Git and shared/synchronized folders.
The examples use `C:/Private/Slipstream` on Windows and
`$HOME/slipstream-private` on Linux. Replace them with your chosen locations.

Create the private parent before running a command. On Windows, check its
Security/Advanced permissions so only your account and required system
administrators can read it. On Linux, use `umask 077` and `mkdir -m 700` for a
new parent directory. Keep the password separately in a password manager.
The tool does not certify parent permissions or disk encryption.

## Verify an existing encrypted backup

Place your `.slbk` file in the chosen private area. Verification authenticates
the archive and checks its format, checksums, types and references. It writes
nothing and prints only anonymous counts and fixed status/error codes.

Windows:

```powershell
.venv-backup/Scripts/python.exe backup.py verify --backup C:/Private/Slipstream/history.slbk
```

Linux:

```bash
.venv-backup/bin/python backup.py verify --backup "$HOME/slipstream-private/history.slbk"
```

Enter the backup password in the hidden prompt. Use a strong, unique password
of 12–1024 UTF-8 bytes; creation asks you to repeat it. Never put passwords or keys into command
arguments, environment variables or files beside the backup. A wrong password,
damaged archive or invalid content returns exit code 2. Stop and review the
fixed error code; never overwrite a file to work around an error.

## Preview, then test recovery

Choose a recovery directory that does not exist. Preview checks it and reports
what would be written. `restore-local` then creates that new private directory.

Windows:

```powershell
.venv-backup/Scripts/python.exe backup.py plan --backup C:/Private/Slipstream/history.slbk --target C:/Private/Slipstream/recovery-test-001
.venv-backup/Scripts/python.exe backup.py restore-local --backup C:/Private/Slipstream/history.slbk --target C:/Private/Slipstream/recovery-test-001
```

Linux:

```bash
.venv-backup/bin/python backup.py plan --backup "$HOME/slipstream-private/history.slbk" --target "$HOME/slipstream-private/recovery-test-001"
.venv-backup/bin/python backup.py restore-local --backup "$HOME/slipstream-private/history.slbk" --target "$HOME/slipstream-private/recovery-test-001"
```

Preview must leave the target absent. A completed recovery has
`RESTORE-COMPLETE`, restored historical objects under `objects/`, and a private
manifest. An interruption leaves `RESTORE-INCOMPLETE`; choose a new target for
a later attempt. Restored objects and their references preserve the archived
history. Analyses are marked `ready: false` because external telemetry is
excluded. This tests a local historical copy; production restore is unsupported.
Existing files and directories are never overwritten.

## Create from an approved offline source

Creation requires an existing local source tree with the exact canonical
namespaces described in [the coverage documentation](PRIVATE_BACKUP.md#exact-coverage).
It does not download that source. Use synthetic data or previously approved
offline objects. In your private area, create `scope.json` with the explicit
activities to include. This example identifier is invented:

```json
[{"year":"2026","activity_id":"900001"}]
```

All canonical profile versions are included; context and analyses cover only
the declared activities. An empty selection `[]` covers profiles only.
Omitted activities are outside this backup's scope.

Windows:

```powershell
.venv-backup/Scripts/python.exe backup.py create --source C:/Private/Slipstream/source --scope C:/Private/Slipstream/scope.json --output C:/Private/Slipstream/history.slbk
```

Linux:

```bash
.venv-backup/bin/python backup.py create --source "$HOME/slipstream-private/source" --scope "$HOME/slipstream-private/scope.json" --output "$HOME/slipstream-private/history.slbk"
```

Verify each newly created backup, then preview and test recovery as above.
The fixed limits remain 1,000 objects, 100 activities, 8 MiB stored total,
16 MiB decoded total, and a 17 MiB archive; Argon2id uses 64 MiB. These are
bounded operations, with no increase to existing hosted resource budgets.
See [the full limits](PRIVATE_BACKUP.md#fixed-resource-limits).

## Use another working directory and keep a separate copy

The commands above run from the source folder. From another folder, use the
full paths to both its Python interpreter and `backup.py`, for example:

```powershell
C:/Tools/slipstream-extended/.venv-backup/Scripts/python.exe C:/Tools/slipstream-extended/backup.py verify --backup C:/Private/Slipstream/history.slbk
```

Copy only the encrypted `.slbk` file to your chosen separate private storage
and run `verify` on that copy independently. Keep its password separately;
keep plaintext recovery folders private. Two copies on one device do not
protect against losing that device. This tool sets no automatic retention or
copying policy.

The backup covers user profiles, RPE/conditions/notes and immutable historical
analyses in the declared scope. It excludes credentials, raw FIT/TCX, GPS and
the rest of the R2 bucket. Live R2 reading uses a separately approved bounded
pilot, documented in [the detailed guide](PRIVATE_BACKUP.md#manually-approved-live-pilot).
Never publish backup files, private selection values, recovery folders or
passwords in Git, Actions artifacts, PRs or MCP.
