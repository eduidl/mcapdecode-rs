# mcaptui

[![crates.io](https://img.shields.io/crates/v/mcaptui.svg)](https://crates.io/crates/mcaptui)

`mcaptui` is a terminal UI for browsing MCAP files through `mcapdecode`.
It is meant for the first pass over a file: check which topics exist, open a message,
and inspect the decoded structure without leaving the terminal.

## Installation

Install from crates.io:

```bash
cargo install mcaptui
```

Tagged GitHub Releases also publish prebuilt archives for:

- Linux: `x86_64-unknown-linux-gnu`

## Usage

```bash
mcaptui sample.mcap
mcaptui sample.mcap --topic /imu/data
mcaptui sample.mcap --parallel
```

## What It Shows

- Topic names, message counts, and schema names in one list
- Decoded message details for the selected record
- The normalized `mcapdecode` field schema used during decoding

## Walkthrough

The screenshots below are generated from the real `mcaptui` binary against
`tools/mcaptui/tests/fixtures/readme-demo.mcap`.

### Topic List

When `mcaptui` starts, it opens a topic list screen.
This is the quickest way to see what is in an MCAP file before digging into individual messages.

![mcaptui topic list](https://raw.githubusercontent.com/eduidl/mcapdecode-rs/main/docs/assets/mcaptui/mcaptui-topics.png)

Use `Up/Down` or `j/k` to move through topics, then press `Enter` to open the selected one.

### Schema Popup

Press `s` to toggle the derived schema widget for the selected topic.
`mcaptui` shows the normalized `mcapdecode` field schema that is actually used for decoding,
which makes it easier to confirm how a payload will be interpreted.

![mcaptui schema popup](https://raw.githubusercontent.com/eduidl/mcapdecode-rs/main/docs/assets/mcaptui/mcaptui-schema.png)

The schema popup is available from both the topic list screen and the message browser screen.
When it is visible, `Tab` includes it in the focus order so the cursor keys can scroll it.

### Message Browser

The message screen shows a scrollable message list on top and the decoded detail view below.
This lets you move through records while keeping the selected message contents visible.

![mcaptui message browser](https://raw.githubusercontent.com/eduidl/mcapdecode-rs/main/docs/assets/mcaptui/mcaptui-messages.png)

Large `List`, `Array`, and `Map` values in the detail pane are truncated after the first 32 items or entries.
The pane keeps the original total count and adds an omission row so large payloads stay navigable in the terminal.

## Key Bindings

- `q`: quit
- Topic screen: `Tab`, `Up/Down`, `j/k`, `PageUp/PageDown`, `Home/End`, `s`, `Enter`
- Message screen: `Esc`, `Tab`, `Up/Down`, `PageUp/PageDown`, `Home/End`, `s`
- Mouse wheel scrolls the pane under the pointer
