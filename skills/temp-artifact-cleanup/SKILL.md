---
name: temp-artifact-cleanup
description: Use after debugging, smoke tests, or local validation to remove safe temporary artifacts before final handoff.
---

# Temp Artifact Cleanup

Use this skill when local debugging, UI smoke runs, previews, or validation steps leave behind temporary files that should not stay in the worktree.

## Goal

Leave `git status --short` clean except for the intended tracked changes.

## Default Workflow

1. Inspect the worktree first.
   - Run `git status --short`.
   - Identify untracked files created by the current task.

2. Remove only safe temporary artifacts.
   - Delete obvious debug, preview, smoke-test, and one-off runtime files.
   - Typical examples include `.audit.log.jsonl`, temporary screenshots, preview logs, Playwright traces, and ad hoc exported files created only for inspection.

3. Protect user and product files.
   - Do not delete tracked files.
   - Do not delete untracked files with unclear ownership or lasting product value.
   - If a file could be a user asset, fixture, dataset, or reference input, stop and ask before removing it.

4. Recheck the worktree.
   - Run `git status --short` again.
   - Confirm only intended repo changes remain.

5. Apply this before final handoff.
   - If temp artifacts were created during validation, clean them before the final summary.
   - If a commit was already created, remove the temp artifacts before the final push or handoff so the worktree is still clean.

## Safe Examples

- `.audit.log.jsonl`
- temporary browser screenshots created for one-off UI inspection
- local preview logs or debug dumps
- transient exported JSON, HTML, or text files created only to inspect runtime state

## Do Not Remove

- tracked repository files
- user-provided fixtures or datasets
- downloaded references still needed for the task
- any untracked file whose purpose is not obvious
