# Repository Instructions

These instructions apply to the entire repository.

## Working Style

- Read the relevant files before editing and keep changes scoped to the user's request.
- Do not revert or overwrite unrelated local changes.
- Prefer the smallest complete fix that satisfies the request.

## Validation

- After making code changes, run the smallest relevant verification you can for the affected area.
- If verification cannot be run, say so clearly in the final response.

## Git Workflow

- Unless the user explicitly says not to commit, the agent may create a git commit after completing the requested code changes.
- Only commit when the changes for the request are complete enough to hand off.
- Do not include unrelated modified files in the commit.
- Do not create empty commits unless the user explicitly asks for one.
- Do not push, merge, rebase, or rewrite history unless the user explicitly asks.

## Commit Messages

- Use the user's request as the source for the commit message.
- Summarize the user's command in a short, specific commit title.
- Prefer imperative, high-signal messages such as `Add checkout retry logging` or `Fix broken CSV export path`.
- Avoid generic messages such as `update`, `fix stuff`, or `changes`.

## Communication

- In the final response, briefly state what changed, what was verified, and whether a commit was created.
