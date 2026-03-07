# Repository Instructions

These instructions apply to the entire repository.

## Working Style

- Read the relevant files before editing and keep changes scoped to the user's request.
- Do not revert or overwrite unrelated local changes.
- Prefer the smallest complete fix that satisfies the request.

## Validation

- After making code changes, run the relevant local tests for the affected area before handoff.
- If no dedicated tests exist for the changed area, run the closest meaningful local validation instead.
- After any test session or local smoke run, terminate every process spawned for that session even if the test failed or was interrupted, and clean up transient runtime artifacts left by the session when practical.
- If verification cannot be run, say so clearly in the final response.

## Git Workflow

- Unless the user explicitly says not to commit, the agent should automatically create a git commit after completing the requested changes and passing relevant local verification.
- Only commit when the changes for the request are complete enough to hand off.
- Do not include unrelated modified files in the commit.
- Do not create empty commits unless the user explicitly asks for one.
- Unless the user explicitly says not to push, the agent should automatically push the completed commit to the current checked-out branch after relevant local verification passes.
- Do not push if tests or validation fail, or if the working tree still contains unrelated changes that should not be published.
- Do not merge, rebase, or rewrite history unless the user explicitly asks.
- Never alter git history or the git log without explicit user confirmation. This includes rebases, resets, amends, history rewrites, or deleting commits.
- Never use force push (`git push --force`, `git push --force-with-lease`) without explicit user confirmation.
- Never delete the `main` branch without explicit user confirmation.
- Never delete merge commits, undo merges, or delete merged branches without explicit user confirmation.

## Commit Messages

- Use the user's request as the source for the commit message.
- Summarize the user's command in a short, specific commit title.
- Prefer imperative, high-signal messages such as `Add checkout retry logging` or `Fix broken CSV export path`.
- Avoid generic messages such as `update`, `fix stuff`, or `changes`.

## Communication

- In the final response, briefly state what changed, what was verified, and whether a commit and push were performed.
