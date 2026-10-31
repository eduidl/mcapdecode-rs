//! An opened MCAP file.

use std::{fs, path::Path};

use memmap2::Mmap;

use crate::error::McapReaderError;

/// A memory-mapped MCAP file and its summary section.
pub(crate) struct McapFile {
    pub(crate) mmap: Mmap,
    pub(crate) summary: mcap::read::Summary,
}

impl McapFile {
    pub(crate) fn open(path: &Path) -> Result<Self, McapReaderError> {
        let file = fs::File::open(path)?;
        let mmap = unsafe { Mmap::map(&file) }?;
        let summary =
            mcap::read::Summary::read(&mmap)?.ok_or(McapReaderError::SummaryNotAvailable)?;
        Ok(Self { mmap, summary })
    }
}
