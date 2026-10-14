# Issue tracker: GitHub

Engineering issues and issue-based specs live in GitHub Issues for the verified repository of the current checkout. The maintainer's master plan owns cross-task priorities; setting up this tracker does not import or duplicate existing private planning material.

## Repository and authority

- Inspect the origin fetch URL and Git configuration to identify the intended GitHub owner/repository. A disabled push URL is an intentional guard, not a reason to alter the remote.
- Use the gh CLI with an explicit --repo OWNER/REPO on issue operations after verifying the target. If origin is a local backup or does not identify the intended GitHub repository, ask for the target before writing.
- Read-only issue inspection is allowed within the task. Creating, editing, labelling, assigning, commenting, closing and linking issues require user authorization for that operation and repository.
- A skill instruction to publish, claim or resolve a ticket does not supply that authorization. Prepare a draft when write authorization is absent.
- Keep private assessment reports and operational details in private planning or the authorized private repository. Never transfer them automatically to a public tracker.
- Follow SECURITY.md for vulnerability reporting. Tokens, sessions, private fitness data and raw provider exceptions do not belong in issue bodies or comments.

## Conventions

Replace OWNER/REPO with the verified target; these commands describe operations, not permission to execute them.

- Read: `gh issue view NUMBER --repo OWNER/REPO --json number,title,body,labels,comments`.
- List: `gh issue list --repo OWNER/REPO --state open --json number,title,body,labels`, with task-relevant filters and bounded result limits.
- Create when authorized: `gh issue create --repo OWNER/REPO --title "..." --body-file BODY_FILE`.
- Comment when authorized: `gh issue comment NUMBER --repo OWNER/REPO --body-file BODY_FILE`.
- Label when authorized: `gh issue edit NUMBER --repo OWNER/REPO --add-label "..."` or `--remove-label "..."`.
- Close when authorized: `gh issue close NUMBER --repo OWNER/REPO --comment "..."`.

GitHub issues and PRs share a number space. Resolve the object type before applying issue operations. For parent/child links or dependencies, check the installed CLI/API support and obtain write authorization; use textual references when native linking is unavailable.

## Pull requests as a triage surface

**PRs as a request surface: no.**

## When a skill requests a tracker action

- Fetch a relevant ticket: read the issue in the verified repository.
- Publish a spec or task: create an issue only with authorization; otherwise return a draft.
- Claim, resolve or change state: apply the requested action only with authorization. Triage readiness is not release approval.
