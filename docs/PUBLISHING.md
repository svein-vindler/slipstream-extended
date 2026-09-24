# Publishing a sanitized public repository

Do not make an existing private Slipstream repository public merely because the
current tree contains no generated data. Older commits may still contain
activity and health CSV files, account-specific configuration or accidentally
committed credentials.

## Recommended approach: a fresh public history

Keep the operational repository private as an archive and publish a new
repository whose first commit is a reviewed snapshot of the clean source tree.
This is safer than rewriting years of refs, pull requests and cached objects.

The recommended public name is `slipstream-extended`. Keep operational resource
names such as the `slipstream-mcp` Worker and `slipstream-data` bucket unchanged;
renaming those would add migration risk without improving the public project.

1. Finish and test the release-readiness branch in the private repository.
2. Run `python scripts/public_release_check.py`. This checks the tracked tree for
   generated fitness files, local configuration, session tokens, legacy
   authentication instructions and installation-specific Worker settings. CI
   runs the same gate on every pull request.
3. Export only tracked files from the reviewed commit with `git archive`.
4. Initialize a new repository in the exported directory.
5. Run tests and a secret scanner against that directory.
6. Create one clean initial commit and push it to the new
   `slipstream-extended` public repository.
7. Enable private vulnerability reporting, Dependabot and branch protection.
8. Create a test installation from the public instructions using separate test
   resources before announcing it.

Because the public repository has a clean history, GitHub will not display it as
a fork of the original project. Attribution remains explicit in the README and
license. `.upstream-version` records the last original-project revision that was
manually reviewed. The weekly workflow compares that marker with upstream and
opens an issue when it changes; it deliberately never merges unrelated history.

After the first release, use the documented
[private-canary promotion workflow](MAINTAINER_WORKFLOW.md) for ongoing changes.
Do not replace the public repository with later private snapshots or attempt to
merge the unrelated histories.

If the desired public repository name is already used by the private instance,
rename the private repository first or choose a new public name. Repository
renames and publication are external, visible changes and should be performed
only after backups and an explicit decision about naming.

## Why deleting files is insufficient

`git rm data/activities.csv` removes the file from later commits, but every blob
referenced by an older commit remains retrievable after a repository becomes
public. Closing pull requests and deleting branches may also leave references
or cached review data.

## Alternative: history rewrite

`git filter-repo` can remove paths from every branch and tag, followed by a
force-push. This invalidates commit hashes, disrupts clones and pull requests,
and still requires checking GitHub caches and any existing forks. Use it only
when preserving the same repository identity is more important than retaining
history, and make a verified backup first.

## Final publication checklist

- Current and historical secret scan is clean.
- `python scripts/public_release_check.py` passes on the exact commit exported.
- No personal fitness data exists in any public commit.
- `worker/wrangler.jsonc` contains no personal hostname, account ID or repo.
- R2 bucket is private and uses Standard storage.
- Access protects the entire Worker with exact-email Allow policy.
- Managed OAuth permits only required redirects; localhost/loopback are off.
- `ACCESS_TEAM_DOMAIN`, `ACCESS_AUD`, `MCP_HOSTNAME` and any optional write
  enablement exist only as Worker secrets.
- GitHub Actions use least privilege and pinned action commits.
- CI, dependency audits and the installation smoke test pass.
- README clearly states self-hosted, single-tenant and user-funded ownership.
- The repository is marked as a **Template repository** in GitHub settings.
- Private vulnerability reporting and Issues are enabled; the latter receives
  upstream-review notifications.
- The default branch requires the CI checks before merge.
- The repository permits rebase merges only; squash merges are disabled so
  GitHub cannot replace a sanitized author with the account's primary email.
- Author and committer addresses on the merged `main` commits use publishable
  GitHub `noreply` identities.
- Create the first signed release tag only after the clean-repository smoke test.
