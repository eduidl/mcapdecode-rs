---
name: github-pull-request
description: Create a GitHub pull request whose description follows the repository's template, or write and revise a PR description. Use when asked to open or create a PR, or to draft or rewrite a PR title or description.
---

# GitHub pull request

Read the actual changes before writing: run `git log main..HEAD` and `git diff main...HEAD`, and do not rely on commit titles alone. Collect the validation that was actually run in this session or by the user, with its results, leaving out checks that CI already runs; never list a command that was not run, and state explicitly what was not tested.

Write the description using [the PR description format](references/format.md). Write the complete body to a temporary file, then review it against the diff: every listed change must exist in the diff, and every user-visible or API change in the diff must be listed. Use a title in English, in the imperative mood, without a trailing period, and at most about 70 characters, matching the repository's previous PR titles.

A request to create or update a PR is the approval to push and publish: do not show the draft and wait for confirmation. For draft-only requests, present the draft without pushing or creating anything.

Push the branch and run `gh pr create --base main --title <title> --body-file <file>`. To revise an existing PR, use `gh pr edit <number> --title <title> --body-file <file>` instead. Read back the result with `gh pr view <number> --json title,body` to verify it, and report the PR URL with the title and body. Append attribution lines at the end of the body only when the session's instructions require them.
