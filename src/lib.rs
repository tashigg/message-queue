use color_eyre::eyre::eyre;
use color_eyre::eyre::WrapErr;
pub use color_eyre::eyre::{Error, Result};

use crate::args::LogFormat;

pub mod args;

pub fn bootstrap(log_format: LogFormat) -> Result<()> {
    if let Err(e) = dotenvy::dotenv() {
        // Don't die if the file doesn't exist.
        if !e.not_found() {
            return Err(e).context("error reading `.env` file");
        }
    }

    // Enables capturing backtraces on stable
    color_eyre::install()?;

    let builder = tracing_subscriber::fmt::fmt();

    match log_format {
        LogFormat::Full => builder.try_init().map_err(|e| eyre!(e))?,
        LogFormat::Compact => {
            builder.compact().try_init().map_err(|e| eyre!(e))?;
        }
        LogFormat::Pretty => {
            builder.pretty().try_init().map_err(|e| eyre!(e))?;
        }
        LogFormat::Json => builder.json().try_init().map_err(|e| eyre!(e))?,
    }

    Ok(())
}
