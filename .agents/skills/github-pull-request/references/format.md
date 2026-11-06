# PR description format

Use English. Replace every placeholder. The introduction, `## Changes`, and `## Validation` are required; omit `## Notes` when there is nothing to note.

```markdown
<One or two sentences: what this PR changes and why.>

## Changes

- <Concrete change, naming the affected crate, public API, or file.>

## Notes

- <Design decisions, known limitations, behavior changes, and work deferred to later PRs.>

## Validation

- CI passed
- `<command not covered by CI>`: <result, e.g. "50 tests passed">
- <Manual checks, and what was not tested (e.g. Windows).>
```

Rules:

- Write for a reviewer: describe the resulting behavior and API, not the editing steps. Keep each bullet to one point.
- Wrap identifiers, commands, and paths in backticks.
- List breaking changes first in `## Changes`, prefixed with `**Breaking:**`, with migration guidance.
- In `## Validation`, the `CI passed` bullet stands for the checks in `.github/workflows/ci.yml` (`cargo fmt`, `cargo clippy`, and `cargo build` / `cargo test` with `--workspace --all-features`). Do not list those commands, or narrower runs of them such as `cargo test -p mcapdecode --all-features`. List only other checks that were actually run, with their outcomes, such as `--no-default-features` builds, benches, scripts, or manual checks. When CI is the only validation, write just `- CI passed`. Do not use unchecked checkboxes.
- Refer to related PRs and issues as `#<number>`.
- Do not add or ask for CHANGELOG entries; this repository has no CHANGELOG.
