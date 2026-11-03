# Release note format

Use English. Include only sections and components relevant to the release. Repeat component headings within each applicable section. Replace placeholders and remove empty sections. Put breaking removals under Breaking changes, with migration guidance; do not use a separate Removed section.

```markdown
Prebuilt Linux binaries are attached below.

## Breaking changes

### <component>

- <User-visible change and migration guidance.> ([#<number>](https://github.com/eduidl/mcapdecode-rs/pull/<number>))

## Added

### <component>

- <What was added and how to use it.> ([#<number>](https://github.com/eduidl/mcapdecode-rs/pull/<number>))

## Improved

### <component>

- <User-visible improvement.> ([#<number>](https://github.com/eduidl/mcapdecode-rs/pull/<number>))

## Fixed

### <component>

- <Previous problem and corrected behavior.> ([#<number>](https://github.com/eduidl/mcapdecode-rs/pull/<number>))

**Full Changelog**: https://github.com/eduidl/mcapdecode-rs/compare/<previous-tag>...<current-tag>
```

Include the binary introduction only when prebuilt Linux binaries are attached. Link each item to its relevant PRs, and include limitations where they affect users.
