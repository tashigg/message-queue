use std::fmt::Write;
use std::fs;
use std::net::{IpAddr, SocketAddr};
use std::path::{Path, PathBuf};

use clap::Parser;
use color_eyre::eyre;
use color_eyre::eyre::WrapErr;
use serde::Serialize;
use tashi_collections::HashSet;
use tashi_consensus_engine::SecretKey;

use tashi_message_queue::config::Address;
use tashi_message_queue::Result;

/// Conveniently generate an address book file and set of private keys for a dMQ session.
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
#[derive(clap::Parser)]
struct Args {
    #[command(subcommand)]
    command: Command,
}

#[derive(clap::Args)]
struct Common {
    /// The path to write the address book and key PEM files to.
    ///
    /// Will be created if it doesn't already exist.
    ///
    /// An error will be returned if the output directory already exists and is not empty.
    #[clap(long, short = 'O', default_value = "address-book/")]
    output_dir: PathBuf,

    /// If `--output-dir` exists and is not empty, erase its contents first.
    #[clap(long, short = 'f')]
    force: bool,
}

#[derive(clap::Subcommand)]
#[clap(rename_all = "kebab-case")]
enum Command {
    /// Generate the address book from a list of predetermined socket addresses.
    FromList {
        // Needs to be part of the subcommand arguments to parse after the subcommand
        #[clap(flatten)]
        common: Common,

        /// The list of socket addresses (IP:port) to include in the address book.
        ///
        /// An error will be returned if the same address is listed more than once.
        #[clap(required = true)]
        socket_addresses: Vec<SocketAddr>,
    },
    /// Generate the address book from a base IP address and a range of ports.
    ///
    /// The total number of addresses generated will be `port_end - port_start + 1`
    /// (because the range is inclusive).
    FromRange {
        #[clap(flatten)]
        common: Common,

        /// The base IP address which will be affixed with ports in the given range.
        base_address: IpAddr,
        /// The start of the port range (inclusive).
        port_start: u16,
        /// The end of the port range (inclusive).
        port_end: u16,
    },
}

fn main() -> Result<()> {
    let args = Args::parse();

    match args.command {
        Command::FromList {
            socket_addresses,
            common,
        } => {
            eyre::ensure!(!socket_addresses.is_empty(), "address list is empty");
            generate_address_book(
                socket_addresses.into_iter(),
                &common.output_dir,
                common.force,
            )
        }
        Command::FromRange {
            base_address,
            port_start,
            port_end,
            common,
        } => {
            let port_range = port_start..=port_end;
            eyre::ensure!(
                !port_range.is_empty(),
                "port range is empty: [{port_start}, {port_end}]"
            );

            generate_address_book(
                port_range.map(|port| SocketAddr::new(base_address, port)),
                &common.output_dir,
                common.force,
            )
        }
    }
}

fn generate_address_book(
    addresses: impl Iterator<Item = SocketAddr>,
    output_dir: &Path,
    force: bool,
) -> Result<()> {
    if output_dir.exists() {
        let mut read_dir = output_dir.read_dir().wrap_err_with(|| {
            format!(
                "failed to open {} to check its contents",
                output_dir.display()
            )
        })?;

        while let Some(entry) = read_dir
            .next()
            .transpose()
            .wrap_err_with(|| format!("error enumerating contents of {}", output_dir.display()))?
        {
            eyre::ensure!(
                force,
                "Output directory {} exists and is not empty; pass `--force`/`-f` to overwrite its contents",
                output_dir.display()
            );

            let path = entry.path();

            let file_type = entry
                .file_type()
                .wrap_err_with(|| format!("failed to fetch metadata for {}", path.display()))?;

            if file_type.is_dir() {
                fs::remove_dir_all(&path)
            } else {
                fs::remove_file(&path)
            }
            .wrap_err_with(|| format!("error removing {}", path.display()))?;
        }
    }

    fs::create_dir_all(output_dir)
        .wrap_err_with(|| format!("failed to create {}", output_dir.display()))?;

    let mut address_book = String::new();

    let mut address_set = HashSet::with_hasher(Default::default());

    for (i, address) in addresses.enumerate() {
        eyre::ensure!(address_set.insert(address), "Duplicate address: {address}");

        let key = SecretKey::generate();
        let pubkey = key.public_key();

        let pem = key.to_pem();

        let pem_filename = format!("key_{i}.pem");
        let pem_path = output_dir.join(&pem_filename);

        fs::write(&pem_path, pem)
            .wrap_err_with(|| format!("failed to write {}", pem_path.display()))?;

        // Mark each entry with a comment referencing its key file.
        writeln!(address_book, "# {pem_filename}").expect("writing to a String cannot fail");

        // Serialize entries one at a time so we can add a comment above each one.
        #[derive(serde::Serialize)]
        struct AddressBookEntry {
            addresses: [Address; 1],
        }

        AddressBookEntry {
            addresses: [Address {
                key: pubkey,
                addr: address,
            }],
        }
        .serialize(toml::Serializer::new(&mut address_book))
        .expect("failed to serialize AddressBookEntry");

        // Add a blank line between entries for readability.
        address_book.push('\n');
    }

    let address_book_path = output_dir.join("address-book.toml");
    fs::write(&address_book_path, &address_book)
        .wrap_err_with(|| format!("failed to write {}", address_book_path.display()))?;

    Ok(())
}
