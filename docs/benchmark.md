# Benchmarking

Benchmarks run against MCAP files that `mcapbench` generates on demand into a cache
directory, so nothing has to be checked in or downloaded first. Generation is
deterministic: a fixed LCG seed feeds CDR encoding for ROS 2 payloads and
`prost-reflect::DynamicMessage` with a generated `FileDescriptorSet` for protobuf.

## Running benchmarks

```bash
# Create a shareable fixture locally (not checked in).
cargo run -p mcapbench -- --case bytes --encoding ros2idl --output /tmp/bytes.mcap

# All suites (about 1.5 minutes).
cargo bench -p mcapdecode --features arrow --bench decode --bench arrow --bench reader

# Run every benchmark once without measuring, to check they all still execute.
cargo bench -p mcapdecode --features arrow --bench decode --bench arrow --bench reader -- --test

# Save and compare local baselines; CI intentionally does not gate elapsed time.
cargo bench -p mcapdecode --features arrow --bench decode --bench arrow --bench reader -- \
  --save-baseline before-change
cargo bench -p mcapdecode --features arrow --bench decode --bench arrow --bench reader -- \
  --baseline before-change
```

Name the bench targets explicitly: `--benches` also runs the library's unit-test
harness, which rejects Criterion options such as `--noplot`.

Every suite shares `mcapbench::criterion()`: ten samples, 1 s warm-up and a 3 s
measurement target. Pass `--warm-up-time` / `--measurement-time` after `--` to measure
longer when a small difference needs confirming.

## Coverage

The suites measure one function each where possible, keeping setup outside the
measured loop:

- `decode`: `decode/{payload}/{encoding}` times one `TopicDecoder::decode` call on an
  in-memory payload from `mcapbench::fixture`. Payloads are `flat`, `nested`, `bytes`,
  `numeric_array` (ROS 2 IDL and protobuf) and `strings` (ROS 2 IDL only). ROS 2 msg is
  left out because its topic decoder is the same CDR decoder as ROS 2 IDL.
  `build_topic_decoder/{encoding}` times schema parsing and resolution of the `nested`
  schema for ROS 2 IDL, ROS 2 msg and protobuf.
- `arrow`: `to_record_batch/{payload}` times `MessageBatchSchema::to_record_batch` on
  about 4 MiB of already-decoded ROS 2 IDL rows.
- `reader`: the read path over a generated file, for what the reader adds around
  decoding. `sequential` and `parallel` read a zstd file whose every chunk holds the
  topic; `skip_chunks` reads a 1% topic clustered in a few chunks, so the chunk index
  lets the reader skip the rest.

Throughput is the payload bytes handed to the function, except in `reader`, where it
is the file size.

The benchmarks only use APIs unchanged since v0.6.0, so the current `benches/` and
`dev/mcapbench` can be copied onto an older release tag to measure it with the same
case IDs.

## Comparing revisions

Criterion's confidence intervals only cover noise within one run; on a shared host the
same binary can drift by 5% or more between runs. To check for a regression, measure the
reference release and the candidate in the same session, alternating between them:

```bash
python3 dev/mcapbench/bench_compare.py v0.10.0 WORKTREE
```

The script copies the current `benches/` and `dev/mcapbench` onto every revision, builds
each into its own executables under `target/bench-compare`, waits for a cooldown, then
runs the revisions in turn for `--rounds` rounds (default 3), rotating their order each
round so none always runs first; use at least as many rounds as revisions. It prints, per case, the
median of the round means, their range, and the change against the first revision.
`WORKTREE` means the working tree, uncommitted changes included. A change only counts
when the round ranges of the two revisions do not overlap.

Use the latest release as the reference; [v0.10.0 reference numbers](benchmarks/v0.10.0.md)
show the magnitude and round-to-round spread to expect for each case.

## Fixture cache and verification

Fixture names are content-addressed over the schema, payload, and file shape, so editing
the generator invalidates cached files automatically. They are cached in
`$TMPDIR/mcapbench` (override with `MCAPBENCH_FIXTURE_DIR`, for example to keep them off a
RAM-backed `/tmp`) and are not cleaned up automatically. A full benchmark run materializes
every combination at 24 MiB each; reclaim the space with:

```bash
rm -rf "${MCAPBENCH_FIXTURE_DIR:-${TMPDIR:-/tmp}/mcapbench}"
```

`dev/mcapbench/tests/roundtrip.rs` decodes every generated combination back to the sample
it was produced from, so a broken schema or payload fails there rather than inside a
benchmark. These fixture checks are opt-in and do not run as part of a normal
`cargo test --workspace`; run them explicitly with:

```bash
cargo test -p mcapbench --features fixture-tests
```
