# Repository Instructions

These instructions apply to the entire repository.

## Working Style

- Read the relevant files before editing and keep changes scoped to the user's request.
- Do not revert or overwrite unrelated local changes.
- Prefer the smallest complete fix that satisfies the request.
- When a change adds, removes, renames, or materially changes any user-facing product function, workflow, button, form field, or operator flow, update `README.md` and `docs/GITHUB_WIKI_PRODUCT_USER_GUIDE.md` in the same change.
- Use the `wiki-readme-sync` skill when it is available for any user-facing function or workflow change that affects product documentation.
- Never write personal local-environment references into tracked repository files.
- Use repo-relative references such as `docs/...`, `backend/services/...`, or the user's repository URL when an external link is required.
- Do not commit absolute filesystem paths, worktree paths, `.codex` paths, or machine-specific home-directory references into the repository.
- Use the `repo-path-hygiene` skill when it is available for documentation, specs, runbooks, READMEs, wiki-source docs, or any change that introduces or rewrites repository file references.

## UI Minimalism

- Keep operator-facing UI clean, minimal, and task-first.
- Prefer short labels, direct controls, and visible status over persistent instructional paragraphs.
- When extra explanation is helpful but not required to complete the task, move it behind a hover or focus `?` tooltip instead of leaving it always visible.
- Keep critical warnings, validation errors, and live status messages inline only when the user must act on them immediately.
- Use the `skills/ui-clean-minimal/SKILL.md` guidance for UI-heavy changes when you add or revise screens, forms, cards, or control groups.

## Main-Agent Workflow

- For every repo-changing bug fix, feature implementation, refactor, or production code change, the main agent must use a main-agent -> sub-agent workflow. The main agent remains accountable for the final result and must not skip delegation.
- Required sequence for those tasks:
  1. Plan the work before editing, including scope, affected systems, validation, and rollout risk.
  2. Delegate focused implementation or research lanes to sub-agents.
  3. Keep sub-agent scopes disjoint when practical, such as backend, frontend, data or migration, docs, review, or QA.
  4. Review every sub-agent output before integrating it. Delegated output must not be accepted without verification.
  5. Run QA after delegated work is integrated back into the main branch of work, using the most relevant local tests, integration checks, and smoke coverage for the changed behavior.
  6. Perform final integration in the main agent, including conflict resolution, final edits, final validation, commit, and push.
- Minimum delegation expectation:
  1. Use at least one sub-agent for any task that changes repo-tracked files.
  2. Use multiple sub-agents when the work spans more than one major area, such as frontend plus backend, backend plus schema, or product code plus docs.
- This rule applies to both bug fixes and new implementations. The main agent always owns the final handoff summary and final release-quality judgment.

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
