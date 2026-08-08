# mcapdecode

`mcapdecode` decodes MCAP topics into `pyarrow.Table` objects using the Rust
[`mcapdecode`](https://github.com/eduidl/mcapdecode-rs) library.

```python
import mcapdecode
import polars as pl

table = mcapdecode.read("sample.mcap", "/imu/data")
frame = pl.from_arrow(table, rechunk=False)
```

Use `mcapdecode.list_topics("sample.mcap")` to discover available topics.
