use clap::Parser;

use run::RunArgs;

use crate::cli::address_book::AddressBookArgs;

pub mod address_book;
pub mod run;

/// A decentralized message queue powered by the Tashi Consensus Engine.
#[derive(clap::Parser, Debug)]
pub struct Args {
    #[command(subcommand)]
    pub command: Command,
}

#[derive(clap::Subcommand, Clone, Debug)]
pub enum Command {
    /// Start the message broker and connect to a TCE network.
    Run(RunArgs),
    /// Generate an address book file and set of private keys for a dMQ session.
    ///
    /// The command takes as an input some set of socket addresses,
    /// either from a given list (`from-list`)
    /// or enumerated from a base address and range of ports (`from-range`).
    ///
    /// The resulting address book will be written to the given output directory as `address-book.toml`.
    ///
    /// Additionally, secret keys will be generated for each address in the set as `key_N.pem`,
    /// where N is the zero-based index in the set. Comments are added to the `address-book.toml`
    /// to disambiguate.
    AddressBook(AddressBookArgs),
}

#[derive(clap::ValueEnum, Debug, Copy, Clone)]
pub enum LogFormat {
    Full,
    Compact,
    Pretty,
    Json,
}

impl Args {
    pub fn log_format(&self) -> LogFormat {
        match &self.command {
            Command::Run(run_args) => run_args.log,
            Command::AddressBook(args) => args.log_format(),
        }
    }
}

pub fn main() -> crate::Result<()> {
    let args = Args::parse();

    crate::bootstrap(args.log_format())?;

    tracing::debug!("Parsed arguments: {args:?}");

    match args.command {
        Command::Run(args) => run::main(args),
        Command::AddressBook(args) => address_book::main(args),
    }
}
