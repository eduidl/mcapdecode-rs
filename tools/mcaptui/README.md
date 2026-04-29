# mcaptui

`mcaptui` is a terminal UI for browsing MCAP files through `mcapdecode`.

## Usage

```bash
mcaptui sample.mcap
mcaptui sample.mcap --topic /imu/data
mcaptui sample.mcap --parallel
```

## Key Bindings

- `q`: quit
- Topic screen: `Tab`, `Up/Down`, `j/k`, `PageUp/PageDown`, `Home/End`, `s`, `Enter`
- Message screen: `Esc`, `Tab`, `Up/Down`, `PageUp/PageDown`, `Home/End`, `s`
- Mouse wheel scrolls the pane under the pointer

## Schema View

Press `s` to toggle the derived schema widget for the selected topic.
The widget is available on both the topic list screen and the message browser screen.
`mcaptui` shows the normalized `mcapdecode` field schema that is actually used for decoding.
When the schema popup is visible, `Tab` includes it in the focus order so the cursor keys can scroll it.
