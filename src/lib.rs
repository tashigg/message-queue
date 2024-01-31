use color_eyre::eyre::WrapErr;
pub use color_eyre::eyre::{Error, Result};
use tracing_subscriber::util::SubscriberInitExt;

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

    match log_format {
        LogFormat::Full => {
            tracing_subscriber::fmt::try_init()?;
        }
        LogFormat::Compact => {
            tracing_subscriber::fmt().compact().try_init()?;
        }
        LogFormat::Pretty => {
            tracing_subscriber::fmt().pretty().try_init()?;
        }
        LogFormat::Json => tracing_subscriber::fmt().json().try_init()?,
    }

    Ok(())
}
