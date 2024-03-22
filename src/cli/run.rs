use color_eyre::eyre;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use color_eyre::eyre::Context;
use tashi_consensus_engine::sync::quic::QuicSocket;
use tashi_consensus_engine::{Platform, SecretKey};

use crate::cli::LogFormat;
use crate::config;
use crate::config::addresses::Addresses;
use crate::config::users::{AuthConfig, UsersConfig};
use crate::mqtt::broker::{self, MqttBroker};

#[derive(clap::Args, Clone, Debug)]
pub struct RunArgs {
    #[clap(short, long, default_value = "full")]
    pub log: LogFormat,

    #[clap(short = 'L', long, default_value = "0.0.0.0:1883")]
    pub mqtt_listen_addr: SocketAddr,

    #[clap(short = 'T', long, default_value = "0.0.0.0:49213")]
    pub tce_listen_addr: SocketAddr,

    #[command(flatten)]
    pub auth_config: AuthConfig,

    #[command(flatten)]
    pub secret_key: SecretKeyOpt,

    #[command(flatten)]
    pub tls_config: Option<TlsConfig>,

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

#[derive(clap::Args, Debug, Clone)]
pub struct TlsConfig {
    #[clap(long, default_value = "0.0.0.0:8883", required = false)]
    pub mqtt_tls_listen_addr: SocketAddr,
    #[clap(long, required = false)]
    pub dns_name: String,
    #[clap(long, required = false)]
    pub tls_secret_key: String,
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
    let addresses = config::addresses::read(&args.config_dir.join("address-book.toml"))?;

    let mut users = config::users::read(&args.config_dir.join("users.toml"))?;

    // Merge any auth overrides from the command-line.
    users.auth.merge(&args.auth_config);

    if users.by_username.is_empty() && !users.auth.allow_anonymous_login {
        let command = std::env::args()
            .next()
            .unwrap_or_else(|| "tashi-message-queue".to_string());

        eyre::bail!(
            "Broker will be impossible to use in current configuration; \
            no user logins are configured and anonymous login is disallowed by default. \
            Run `{command} user add` to create at least one user login or enable anonymous login. \
            Run `{command} help` for details.",
        )
    }

    eyre::ensure!(!users.by_username.is_empty() || users.auth.allow_anonymous_login,);

    let tce_config =
        create_tce_config(&args, &addresses).wrap_err("error initializing TCE config")?;

    main_async(args, users, tce_config)
}

// `#[tokio::main]` doesn't have to be attached to the actual `main()`, and it can accept args
#[tokio::main]
async fn main_async(
    args: RunArgs,
    users: UsersConfig,
    tce_config: tashi_consensus_engine::Config,
) -> crate::Result<()> {
    let (tce_platform, tce_message_stream) = Platform::start(
        tce_config,
        QuicSocket::bind_udp(args.tce_listen_addr).await?,
        false,
    )?;

    let tce_platform = Arc::new(tce_platform);

    let tls_config = args
        .tls_config
        .map(|tls_config| {
            let tls_socket_addr = tls_config.mqtt_tls_listen_addr;
            let key = SecretKeyOpt {
                secret_key: Some(tls_config.tls_secret_key),
                secret_key_file: None,
            }
            .read_key()?;
            let cert = tashi_consensus_engine::Certificate::generate_self_signed(
                &key,
                tls_socket_addr,
                &tls_config.dns_name,
                None,
            )?;

            let cert = cert.into_rustls();

            std::fs::write("cert.der", cert.0.as_slice())?;

            eyre::Ok(broker::TlsConfig {
                socket_addr: tls_socket_addr,
                cert: Vec::from([cert]),
                key: key.to_rustls()?,
            })
        })
        .transpose()?;

    let mut broker = MqttBroker::bind(
        args.mqtt_listen_addr,
        tls_config,
        users,
        tce_platform.clone(),
        tce_message_stream,
    )
    .await?;

    loop {
        tokio::select! {
            res = broker.run() => {
                res?;
            }

            res = tokio::signal::ctrl_c() => {
                res.wrap_err("error from ctrl_c() handler")?;
                tce_platform.shutdown().await;
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

    Ok(tashi_consensus_engine::Config::new(secret_key)
        .initial_nodes(nodes)
        .report_events_before_consensus())
}
