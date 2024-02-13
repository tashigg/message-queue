use std::net::SocketAddr;
use std::path::PathBuf;

use color_eyre::eyre::Context;
use tashi_consensus_engine::SecretKey;

use crate::cli::LogFormat;
use crate::config::addresses::Addresses;
use crate::mqtt::broker::MqttBroker;

#[derive(clap::Args, Clone, Debug)]
pub struct RunArgs {
    #[clap(short, long, default_value = "full")]
    pub log: LogFormat,

    #[clap(short = 'L', long, default_value = "0.0.0.0:1883")]
    pub mqtt_listen_addr: SocketAddr,

    #[clap(short = 'T', long, default_value = "0.0.0.0:49213")]
    pub tce_listen_addr: SocketAddr,

    #[command(flatten)]
    pub secret_key: SecretKeyOpt,

    #[clap(default_value = "dmq/")]
    pub config_dir: PathBuf,
}

#[derive(clap::Args, Debug, Clone)]
#[group(required = true, multiple = false)]
pub struct SecretKeyOpt {
    /// Read a P-256 secret key from hex-encoded DER bytes.
    #[clap(short = 'k', long, env)]
    pub secret_key: Option<String>,

    /// Read a P-256 secret key from a PEM-encoded file.
    #[clap(short = 'f', long, env)]
    pub secret_key_file: Option<PathBuf>,
}

impl SecretKeyOpt {
    /// NOTE: uses blocking I/O internally if the secret key was specified as a file.
    pub fn read_key(&self) -> crate::Result<SecretKey> {
        if let Some(der) = &self.secret_key {
            let der_bytes = hex::decode(der).wrap_err("error decoding hex-encoded secret key")?;
            return SecretKey::from_der(&der_bytes)
                .wrap_err("error decoding P-256 secret key from DER");
        }

        if let Some(path) = &self.secret_key_file {
            // There's no benefit to using `tokio::fs` because it just does the blocking work on a background thread.
            let pem = std::fs::read(path)
                .wrap_err_with(|| format!("error reading {}", path.display()))?;

            return SecretKey::from_pem(&pem).wrap_err_with(|| {
                format!("error reading P-256 secret key from {}", path.display())
            });
        }

        unreachable!("BUG: Clap should have required one of `--secret-key` or `--secret-key-file`")
    }
}

pub fn main(args: RunArgs) -> crate::Result<()> {
    // File and stdio aren't truly async in Tokio so we might as well do that before we even start the runtime
    let addresses =
        crate::config::addresses::read(&args.config_dir).wrap_err("error reading config")?;

    let tce_config =
        create_tce_config(&args, &addresses).wrap_err("error initializing TCE config")?;

    main_async(args, tce_config)
}

// `#[tokio::main]` doesn't have to be attached to the actual `main()`, and it can accept args
#[tokio::main]
async fn main_async(
    args: RunArgs,
    _tce_config: tashi_consensus_engine::Config,
) -> crate::Result<()> {
    let mut broker = MqttBroker::bind(args.mqtt_listen_addr).await?;

    loop {
        tokio::select! {
            res = broker.run() => {
                res?;
            }

            res = tokio::signal::ctrl_c() => {
                res.wrap_err("error from ctrl_c() handler")?;
                break;
            }
        }
    }

    tracing::info!(
        "Ctrl-C received; waiting for {} connections to close",
        broker.connections()
    );

    broker.shutdown().await
}

/// NOTE: uses blocking I/O internally.
fn create_tce_config(
    args: &RunArgs,
    addresses: &Addresses,
) -> crate::Result<tashi_consensus_engine::Config> {
    let secret_key = args.secret_key.read_key()?;

    let nodes = addresses
        .addresses
        .iter()
        .map(|address| (address.key.clone(), address.addr))
        .collect();

    Ok(tashi_consensus_engine::Config::new(secret_key).initial_nodes(nodes))
}
