use color_eyre::eyre::eyre;
use color_eyre::eyre::WrapErr;
pub use color_eyre::eyre::{Error, Result};
use tracing_subscriber::EnvFilter;

pub use tashi_consensus_engine::{flatten_task_result, map_join_error};

use crate::cli::LogFormat;

pub mod cli;
pub mod collections;

pub mod config;

pub mod mqtt;

pub mod password;

pub mod transaction;

pub fn bootstrap(log_format: LogFormat) -> Result<()> {
    if let Err(e) = dotenvy::dotenv() {
        // Don't die if the file doesn't exist.
        if !e.not_found() {
            return Err(e).context("error reading `.env` file");
        }
    }

    // Enables capturing backtraces on stable and adds color codes.
    color_eyre::install()?;

    let builder = tracing_subscriber::fmt::fmt().with_env_filter(EnvFilter::from_default_env());

    match log_format {
        // These all result in different typestate
        LogFormat::Full => builder.try_init(),
        LogFormat::Compact => builder.compact().try_init(),
        LogFormat::Pretty => builder.pretty().try_init(),
        LogFormat::Json => builder.json().try_init(),
    }
    .map_err(|e| eyre!(e))?;

    Ok(())
}
