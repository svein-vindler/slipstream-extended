# Maintainer workflow: private canary to public release

Slipstream Extended deliberately separates a private operational installation
from the public source repository:

- The **private canary repository** owns GitHub secrets, scheduled Garmin jobs,
  deployment history and the maintainer's live Cloudflare/R2 installation.
- The **public repository** contains portable source, tests and documentation.
  It never receives fitness data, account-specific configuration or deployment
  secrets.

The repositories have independent Git histories. Never merge their branches or
automatically synchronize their complete trees. Promote small, reviewed commits
with `cherry-pick` instead.

## One-time local remotes

In the private clone, add the public repository as a fetch-only remote:

```bash
git remote add public git@github.com:<owner>/slipstream-extended.git
git remote set-url --push public DISABLED
```

In a separate public clone, add the private canary as a fetch-only remote:

```bash
git remote add canary git@github.com:<owner>/<private-install-repo>.git
git remote set-url --push canary DISABLED
```

Confirm the URLs with `git remote -v` before fetching or pushing. Only each
clone's `origin` may be writable.

Configure the public clone with a publishable Git identity. A GitHub `noreply`
address prevents the maintainer's private email from becoming part of the public
commit history:

```bash
git config user.name "<GitHub login>"
git config user.email "<GitHub user ID>+<GitHub login>@users.noreply.github.com"
```

## Develop and validate privately

1. Create a focused feature branch in the private canary repository.
2. Keep secrets, generated Garmin data and local environment files untracked.
3. Add tests and portable documentation with the implementation.
   Keep README's project purpose, current capabilities and refresh behavior
   aligned with the implementation in both repositories. Include the relevant
   usage, scope and recovery details in the focused documentation.
4. Run Python lint/tests, Worker typecheck/tests and the public-release check.
5. Merge through a private pull request after CI passes.
6. If runtime behavior changed, deploy the private Worker or workflow and
   validate it against the maintainer's installation.
7. Observe at least one representative run before promoting high-risk changes.

Changes to authentication, R2 schemas, backfill state, refresh coordination or
Garmin request volume require proportionally longer validation than a pure
documentation change.

## Promote a tested change to public

From the public clone:

```bash
git fetch canary main
git switch -c feature/<portable-change>
git cherry-pick <reviewed-portable-commit>
```

`cherry-pick` preserves the original author identity. Inspect both author and
committer before pushing:

```bash
git log -1 --format='%an <%ae> | %cn <%ce>'
```

If the private commit used an email that should not be public, replace its author
with the public clone's configured identity. Keep the message portable and omit
private-origin SHA trailers and operational identifiers:

```bash
git commit --amend --no-edit --reset-author
```

Then review the complete diff. Remove or parameterize anything specific to the
private installation, including account identifiers, hostnames, repository
names, email addresses, Garmin IDs and operational defaults that do not suit
other users.

Before opening the public pull request, run:

```bash
python scripts/public_release_check.py
ruff check .
pytest -q
cd worker
npm ci
npm audit --omit=dev
npm run typecheck
npm test
```

The public pull request must explain how the change was validated in the canary
and whether it changes storage, request volume, permissions or free-tier usage.
Merge only after public CI passes. Use **Rebase and merge**, not **Squash and
merge**: GitHub may assign the pull-request author's account email to a squash
commit even when every branch commit uses a `noreply` address. The public
repository should therefore allow rebase merges only and require linear
history.

After merging, fetch `main` and inspect the resulting commits before tagging a
release:

```bash
git log -3 --format='%h %an <%ae> | %cn <%ce>'
```

Stop publishing if any author or committer address is not intended to be
public. Changing a later commit does not remove an address from existing Git
history.

## Clean up completed branches

After a successful PR merge and release verification, delete its completed
remote branch and local branch when no worktree still uses it. Confirm the tip
is merged or its patches are preserved in the merged result when squash/rebase
was used. A commit by itself does not finish a branch: retain unmerged work and
branches checked out in other worktrees. Never merge a completed branch again
just to remove it. Keep private and public cleanup separate.

## Bring public maintenance back to the canary

Dependency, documentation and contributor changes may originate in public. Test
them there first, then apply the resulting squash commit to a private branch:

```bash
git fetch public main
git switch -c maintenance/public-<short-description>
git cherry-pick -x <public-commit-sha>
```

Run private CI before merging. Deploy only when runtime dependencies or Worker
code changed. Cherry-picking does not touch R2 objects, Cloudflare bindings,
Access policies or repository secrets.

## Dependency updates

- Runtime-sensitive updates are tested in the private canary before public
  promotion.
- Security-only or contributor-tooling updates may start in public, then be
  brought back to the canary.
- Never merge a failed Dependabot pull request.
- Major or behavior-changing updates require release-note review and targeted
  runtime tests even when generic CI passes.
- Keep dependency versions aligned between repositories after validation; do
  not maintain silent long-lived forks.

### Garmin FIT SDK licensing and decoder updates

`garmin-fit-sdk` is an external dependency, not SDK code distributed under this
project's MIT license. Version 21.218.0 includes Garmin's
[FIT Protocol License Agreement](https://github.com/garmin/fit-python-sdk/blob/7e4b3132d50de66f5adfcd6c0addeaade41536d4/LICENSE.txt).
Installers must review that agreement for their intended use, including its
distribution and SDK-benchmarking restrictions. This project does not vendor or
relicense the SDK; an installation reference is not a general redistribution
grant or a legal compatibility guarantee.

Decoder updates must retain the real synthetic FIT field/scaling/sentinel tests,
Coach identity and historical-byte preservation, encrypted-backup compatibility
and authenticated JSON/SSE reader checks. An SDK update must not automatically
regenerate stored analyses or rewrite history.

### Source maps and existing installations

[Wrangler 4.151](https://github.com/cloudflare/workers-sdk/releases/tag/wrangler%404.151.0)
generates/uploads source maps by default for normal deploy.
To preserve non-upload with unchanged configuration, pass
`--no-upload-source-maps` to both dry-run and actual deploy. From `worker/`,
bind the placeholders to the existing installation's reviewed paths and name:

```bash
npm run deploy -- "<reviewed-entry.ts>" --config "<reviewed-config.jsonc>" --name "<worker-name>" --keep-vars --strict --autoconfig=false --no-upload-source-maps --dry-run
npm run deploy -- "<reviewed-entry.ts>" --config "<reviewed-config.jsonc>" --name "<worker-name>" --keep-vars --strict --autoconfig=false --no-upload-source-maps
```

Use the candidate's own pinned Wrangler and retain its actual bindings,
environment, resources and approved output locations. Local map files and a
successful dry-run do not attest remote upload or resource parity. Respect the
cf-output guard; do not bypass it by deleting other build state. Unknown flag
support or configuration drift stops release, without silently accepting the
default upload policy. Opt-out does not remove maps uploaded by earlier releases.

### Native image security override

`worker/package.json` temporarily overrides only Miniflare's `sharp` dependency
to `0.35.5`. The scoped override addresses
[GHSA-wq5f-xc86-pv6w](https://github.com/advisories/GHSA-wq5f-xc86-pv6w)
without changing the parent tools, application dependencies or Worker code.
Keep the regenerated lockfile, including optional platform packages, committed.

Run the full `npm audit` as well as `npm audit --omit=dev`, `npm ci`, typecheck
and both Worker test suites after changes to these tools. The existing CI audit
also includes development dependencies. The native image test
loads Miniflare's actual `sharp`, requires patched librsvg and decodes a tiny
synthetic SVG. It runs on Windows locally and on Linux in existing CI; no live
fitness data or deployment is required.

Remove the override once a compatible Wrangler/Cloudflare Vitest pair supplies
patched `sharp` through Miniflare, then regenerate the lockfile and repeat the
same checks. Do not use forced audit fixes or bypass peer dependencies.

### Cloudflare Agents and MCP compatibility

Agents declares exact peer versions for the MCP SDK, server and client.
Check `npm view agents@<candidate-version> peerDependencies` before upgrading
any of them. Update the supported set together and validate with `npm ci`,
Worker typecheck and runtime tests. Do not bypass a peer conflict with
`--force` or `--legacy-peer-deps`.

For GHSA-6qxp-vccf-f47h, the pinned `agents@0.24.0` has a reviewed, scoped
security exception: its SDK and client peers use the root's patched pins via
npm overrides. Agents and the server remain unchanged. This combination is
locally and CI-tested, not declared supported by upstream Agents. Keep the
issuer-isolation tests and repeat clean installation, full audit, typecheck
and runtime tests. See [security testing](SECURITY_TESTING.md) for scope and
the condition for removing the overrides.

Dependabot groups Agents and MCP candidates together, and groups Wrangler
with the Cloudflare Vitest plugin. Its ignore rules name only known
incompatible MCP versions. When an Agents release supports one of those
versions, remove the matching ignore rule in the same tested change.

## R2 and schema safety

Portable data changes must be backward-compatible with existing installations:

- Treat R2 keys and stored schemas as public interfaces.
- Make migrations resumable and idempotent.
- Preserve old readable versions until a documented migration has completed.
- Keep object, byte and per-run write guards in place.
- Test interrupted and repeated runs.
- Never make code publication conditional on copying the maintainer's R2 data.

The private R2 bucket survives repository changes. Publishing or cherry-picking
source does not copy, delete or recreate stored fitness data.

### Retained hardening and measurement boundaries

The GitHub GET boundary accepts at most 4,194,304 delivered body bytes. Its
15,000 ms owned timer initiates I/O abort, and read/cancel settlement precedes
claim release; this is not a hard parse, CPU, cleanup or return deadline. Invalid
run-list envelopes stay unknown instead of causing a new dispatch. Scalar
detail leaves retain literal names, units, zero/null and provenance; unsupported
compound values fail in a controlled way. Stored/decode guards remain per object,
not a new aggregate/output or legacy archive/XML ceiling. Pipeline R2 reads close
their bodies while preserving an earlier read failure over a later close error.

The 100-profile capacity includes permanent reservations for immutable keys,
including unknown PUT results. Creation can stop below 100 stored profiles;
identical repeats do not refund capacity. There is no automatic expiry, refund,
reclaim or reset. See [fresh-data recovery](FRESH_DATA.md#recovery-and-reader-compatibility)
before considering a rollback across new durable inspection state.

Existing reviewed synthetic Python comparisons and the warm profile-capacity
sequence establish only their selected workload observations. They are historical
measurements, not new counts for the current Wrangler/runtime. Initialization,
full SQL/CPU/wire usage, retries, queue overlap and combined account cost remain
outside that evidence. Neither local tests nor CI establish total performance,
free operation or complete provider/failure coverage. Keep the existing ceilings
until a separately reviewed contract changes them.

## Public workflow privacy

The maintainer's public template repository keeps data-producing workflows
disabled at repository level. CI, Dependabot and the upstream notification may
remain active. A repository created from the template contains the workflow
files, and its owner enables Actions during installation.

After adding or renaming a workflow that can contact Garmin, read deployment
secrets or write R2, immediately verify that it is disabled in the public source
repository. Public Actions logs must never contain the maintainer's operational
activity metadata.

## Never promote

- Garmin session files, raw FIT/TCX exports or generated health/activity data
- `.dev.vars`, `.env`, `.granular/` or other local runtime state
- Cloudflare account IDs, Access values, personal hostnames or email addresses
- GitHub or R2 credentials
- private author or committer email addresses in Git metadata
- one-off recovery files or private-only operational experiments
- full branches or repository archives from the private history

When uncertain, stop and inspect the staged tree with
`python scripts/public_release_check.py` before pushing anything public.
