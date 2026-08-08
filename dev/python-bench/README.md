# Python Benchmark

This manual benchmark compares `mcapdecode.read()` with pure-Python
[`rosbags`](https://ternaris.gitlab.io/rosbags/) for the complete path from a
ROS 2 MCAP topic to a `pyarrow.Table`.

```bash
python -m venv .venv
. .venv/bin/activate
pip install -e 'dev/python-bench[test,dev]'
maturin develop --release --manifest-path mcapdecode/mcapdecode-python/Cargo.toml
python dev/python-bench/benchmark.py --output /tmp/mcapdecode-benchmark.json
```

The script generates deterministic 24 MiB ROS 2 `.msg` fixtures for `flat`,
`nested`, `bytes`, `numeric-array`, and `strings`. It checks that both readers
produce equal Arrow tables before timing ten warm iterations, then writes JSON
and Markdown reports. `p95_ms` uses linear interpolation between adjacent
ordered samples. The results are machine-specific and are not CI gates.
