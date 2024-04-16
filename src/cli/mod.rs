use clap::Parser;

use run::RunArgs;

use crate::cli::address_book::AddressBookArgs;
use crate::cli::user::UserArgs;

pub mod address_book;
pub mod run;

pub mod user;

/// A decentralized message queue powered by the Tashi Consensus Engine.
#[derive(clap::Parser, Debug)]
pub struct Args {
    #[command(subcommand)]
    pub command: Command,
}

#[derive(clap::Subcommand, Clone, Debug)]
pub enum Command {
    /// Start the message broker and connect to a FoxMQ cluster.
    Run(RunArgs),
    /// Generate an address book file and set of private keys for a FoxMQ session.
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
    /// Manage user credentials for connecting to the message broker protocol.
    User(UserArgs),
}

#[derive(clap::ValueEnum, Debug, Copy, Clone)]
pub enum LogFormat {
    /// Emit human-readable single line logs for each event.
    Full,
    /// A variant of full, optimized for shorter line lengths.
    Compact,
    /// Format events in multi-line very "prettified" form.
    Pretty,
    /// Emit JSON-lines formatted events.
    Json,
}

impl Args {
    pub fn log_format(&self) -> LogFormat {
        match &self.command {
            Command::Run(args) => args.log,
            Command::AddressBook(args) => args.log_format(),
            Command::User(args) => args.log_format(),
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
        Command::User(args) => user::main(args),
    }
}
