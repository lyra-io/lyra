# Project Instructions

## Pull-request-only workflow

- All changes must reach the default branch through a GitHub pull request, including code, documentation, automation, dependency updates, and upstream synchronization.
- Never commit or push directly to `main`, `master`, or another default or protected integration branch.
- Work on a topic branch using a conventional prefix such as `feat/`, `fix/`, `docs/`, or `chore/`; never use `codex/`.
- Push the topic branch and open or update a pull request. Do not merge or enable auto-merge unless the user explicitly requests it.
- This policy applies to contributors, administrators, and automation. Do not bypass or disable branch protections to land changes.
- Require pull requests on the default branch without bypass actors where GitHub supports enforcement. If the repository's plan prevents enforcement, report the gap and still follow this workflow; do not change its visibility or billing plan without explicit approval.
