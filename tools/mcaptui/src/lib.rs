pub mod app;
pub mod format;

mod loader;
mod runtime;
mod schema;
mod terminal;
mod ui;

use anyhow::Result;

pub fn main() -> Result<()> {
    runtime::main()
}
