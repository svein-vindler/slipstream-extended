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
git cherry-pick -x <private-commit-sha>
```

`cherry-pick` preserves the original author identity. Inspect both author and
committer before pushing:

```bash
git log -1 --format='%an <%ae> | %cn <%ce>'
```

If the private commit used an email that should not be public, replace its author
with the public clone's configured identity while retaining the commit message
and `cherry picked from` trailer:

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
