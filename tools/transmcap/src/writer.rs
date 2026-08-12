use std::{
    fs,
    io::{self, BufWriter, Write},
    path::{Path, PathBuf},
};

use anyhow::Result;
use arrow::record_batch::RecordBatch;
use mcapdecode::{JsonlWriter as McapJsonlWriter, NonFiniteFloats};

pub trait RecordBatchWriter {
    fn write_batch(&mut self, batch: RecordBatch) -> Result<()>;
    fn finish(&mut self) -> Result<()>;
}

// --- JSON Lines ---

/// JSON Lines writer whose handling of non-finite floats is selected by
/// [`NonFiniteFloats`].
pub struct JsonlWriter {
    inner: McapJsonlWriter<Box<dyn Write>>,
    flush_each_batch: bool,
}

impl JsonlWriter {
    pub fn new(output: Option<&Path>, non_finite_floats: NonFiniteFloats) -> Result<Self> {
        let flush_each_batch = output.is_none();
        let dest: Box<dyn Write> = match output {
            Some(path) => Box::new(BufWriter::new(fs::File::create(path)?)),
            None => Box::new(BufWriter::new(io::stdout().lock())),
        };
        Ok(Self {
            inner: McapJsonlWriter::new(dest, non_finite_floats),
            flush_each_batch,
        })
    }
}

impl RecordBatchWriter for JsonlWriter {
    fn write_batch(&mut self, batch: RecordBatch) -> Result<()> {
        self.inner.write(&batch)?;
        if self.flush_each_batch {
            self.inner.flush()?;
        }
        Ok(())
    }

    fn finish(&mut self) -> Result<()> {
        self.inner.finish()?;
        self.inner.flush()?;
        Ok(())
    }
}

// --- CSV ---

pub struct CsvWriter {
    dest: Box<dyn Write>,
    header_written: bool,
    flush_each_batch: bool,
}

impl CsvWriter {
    pub fn new(output: Option<&Path>) -> Result<Self> {
        let flush_each_batch = output.is_none();
        let dest: Box<dyn Write> = match output {
            Some(path) => Box::new(BufWriter::new(fs::File::create(path)?)),
            None => Box::new(BufWriter::new(io::stdout().lock())),
        };
        Ok(Self {
            dest,
            header_written: false,
            flush_each_batch,
        })
    }
}

impl RecordBatchWriter for CsvWriter {
    fn write_batch(&mut self, batch: RecordBatch) -> Result<()> {
        let mut csv_writer = if self.header_written {
            arrow::csv::WriterBuilder::new()
                .with_header(false)
                .build(&mut self.dest)
        } else {
            self.header_written = true;
            arrow::csv::WriterBuilder::new()
                .with_header(true)
                .build(&mut self.dest)
        };
        csv_writer.write(&batch)?;
        drop(csv_writer);
        if self.flush_each_batch {
            self.dest.flush()?;
        }
        Ok(())
    }

    fn finish(&mut self) -> Result<()> {
        self.dest.flush()?;
        Ok(())
    }
}

// --- Parquet ---

pub struct ParquetWriter {
    output_path: PathBuf,
    inner: Option<parquet::arrow::ArrowWriter<fs::File>>,
    wrote_any_batch: bool,
}

impl ParquetWriter {
    pub fn new(output: &Path) -> Result<Self> {
        Ok(Self {
            output_path: output.to_path_buf(),
            inner: None,
            wrote_any_batch: false,
        })
    }
}

impl RecordBatchWriter for ParquetWriter {
    fn write_batch(&mut self, batch: RecordBatch) -> Result<()> {
        self.wrote_any_batch = true;

        if self.inner.is_none() {
            let file = fs::File::create(&self.output_path)?;
            let props = parquet::file::properties::WriterProperties::builder().build();
            self.inner = Some(parquet::arrow::ArrowWriter::try_new(
                file,
                batch.schema(),
                Some(props),
            )?);
        }

        self.inner.as_mut().unwrap().write(&batch)?;
        Ok(())
    }

    fn finish(&mut self) -> Result<()> {
        if !self.wrote_any_batch {
            anyhow::bail!("No messages found for the selected topic");
        }
        let writer = self
            .inner
            .take()
            .expect("inner must exist after first batch");
        writer.close()?;
        Ok(())
    }
}
