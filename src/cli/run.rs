use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use color_eyre::eyre;
use color_eyre::eyre::Context;
use tashi_consensus_engine::sync::quic::QuicSocket;
use tashi_consensus_engine::{Platform, SecretKey};
use tokio_rustls::rustls;

use crate::cli::LogFormat;
use crate::config;
use crate::config::addresses::Addresses;
use crate::config::users::{AuthConfig, UsersConfig};
use crate::mqtt::broker::{self, MqttBroker};
use crate::mqtt::KeepAlive;

#[derive(clap::Args, Clone, Debug)]
pub struct RunArgs {
    /// Set the format of log output.
    #[clap(short, long, default_value = "full")]
    pub log: LogFormat,

    /// The TCP socket address to listen for MQTT (non-TLS) connections from clients.
    #[clap(short = 'L', long, default_value = "0.0.0.0:1883")]
    pub mqtt_addr: SocketAddr,

    // `19793` is the ASCII characters `MQ` reinterpreted as a big-endian integer.
    /// The UDP socket address to listen for cluster connections from other FoxMQ brokers.
    #[clap(short = 'C', long, default_value = "0.0.0.0:19793")]
    pub cluster_addr: SocketAddr,

    /// Set the maximum Keep Alive interval for MQTT connections, in seconds.
    ///
    /// A client may specify a nonzero interval smaller than this.
    /// This also becomes the default Keep Alive interval if a client does not specify one.
    ///
    /// Per the specification, the connection times out if the client does not send any
    /// MQTT control packet in 1.5x the Keep Alive interval.
    ///
    /// Set to 0 to allow the client to set any Keep Alive interval, including 0 (no timeout).
    ///
    /// The maximum value allowed by the MQTT spec is 65,535, or 18 hours, 12 minutes, and 15 seconds.
    #[clap(long, default_value = "3600")]
    pub max_keep_alive: u16,

    #[command(flatten)]
    pub auth_config: AuthConfig,

    #[command(flatten)]
    pub secret_key: SecretKeyOpt,

    #[command(flatten)]
    pub tls_config: TlsConfig,

    /// The directory containing `address-book.toml` and (optionally) `users.toml`.
    #[clap(default_value = "foxmq.d/")]
    pub config_dir: PathBuf,
}

#[derive(clap::Args, Debug, Clone)]
#[group(required = true, multiple = false)]
pub struct SecretKeyOpt {
    /// Read the P-256 secret key used to identify this broker in the cluster from hex encoded DER.
    ///
    /// If `--tls-key-file` is not provided and `--mqtts` is enabled,
    /// this or `--secret-key-file` will be used by default.
    #[clap(short = 'k', long, env)]
    pub secret_key: Option<String>,

    /// Read the PEM-encoded P-256 secret key used to identify this broker in the cluster from a file.
    #[clap(short = 'f', long, env)]
    pub secret_key_file: Option<PathBuf>,
}

#[derive(clap::Args, Debug, Clone)]
pub struct TlsConfig {
    /// Enable listening for MQTT-over-TLS connections on a separate socket (0.0.0.0:8883 by default).
    #[clap(long)]
    pub mqtts: bool,

    /// The TCP socket address to listen for MQTT-over-TLS (`mmqts`) connections from clients.
    #[clap(long, default_value = "0.0.0.0:8883")]
    pub mqtts_addr: SocketAddr,

    /// The domain name to report for Server Name Identification (SNI) in TLS.
    #[clap(long, default_value = "foxmq.local")]
    pub server_name: String,

    /// Override the secret key used for TLS handshakes.
    ///
    /// Defaults to the main secret key (`--secret-key`/`--secret-key-file`).
    #[clap(long)]
    pub tls_key_file: Option<PathBuf>,

    /// Path to the X.509 certificate to use for TLS.
    ///
    /// Defaults to a certificate self-signed with either `--tls-key-file`
    /// or the main secret key (`--secret-key`/`--secret-key-file`).
    #[clap(long)]
    pub tls_cert_file: Option<PathBuf>,
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
            return read_secret_key(path);
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
            .unwrap_or_else(|| "foxmq".to_string());

        eyre::bail!(
            "Broker will be impossible to use in current configuration; \
            no user logins are configured and anonymous login is disallowed by default. \
            Run `{command} user add` to create at least one user login or enable anonymous login. \
            Run `{command} help` for details.",
        )
    }

    eyre::ensure!(!users.by_username.is_empty() || users.auth.allow_anonymous_login,);

    let secret_key = args.secret_key.read_key()?;

    let tce_config = create_tce_config(secret_key.clone(), &addresses)
        .wrap_err("error initializing TCE config")?;

    let tls_config = args
        .tls_config
        .mqtts
        .then(|| {
            let tls_socket_addr = args.tls_config.mqtts_addr;

            let key = if let Some(secret_key_file) = &args.tls_config.tls_key_file {
                read_secret_key(secret_key_file)?
            } else {
                secret_key
            };

            let cert_chain = if let Some(cert_file) = &args.tls_config.tls_cert_file {
                let cert_pem = std::fs::read(cert_file)
                    .wrap_err_with(|| format!("error reading from {}", cert_file.display()))?;

                let certs = rustls_pemfile::certs(&mut &cert_pem[..]).wrap_err_with(|| {
                    format!(
                        "error reading certificate chain from {}",
                        cert_file.display()
                    )
                })?;

                certs.into_iter().map(rustls::Certificate).collect()
            } else {
                vec![tashi_consensus_engine::Certificate::generate_self_signed(
                    &key,
                    tls_socket_addr,
                    &args.tls_config.server_name,
                    None,
                )?
                .into_rustls()]
            };

            eyre::Ok(broker::TlsConfig {
                socket_addr: tls_socket_addr,
                cert_chain,
                key: key.to_rustls()?,
            })
        })
        .transpose()?;

    main_async(args, users, tce_config, tls_config)
}

// `#[tokio::main]` doesn't have to be attached to the actual `main()`, and it can accept args
#[tokio::main]
async fn main_async(
    args: RunArgs,
    users: UsersConfig,
    tce_config: tashi_consensus_engine::Config,
    tls_config: Option<broker::TlsConfig>,
) -> crate::Result<()> {
    let (tce_platform, tce_message_stream) = Platform::start(
        tce_config,
        QuicSocket::bind_udp(args.cluster_addr).await?,
        false,
    )?;

    let tce_platform = Arc::new(tce_platform);

    let mut broker = MqttBroker::bind(
        args.mqtt_addr,
        tls_config,
        users,
        tce_platform.clone(),
        tce_message_stream,
        KeepAlive::from_seconds(args.max_keep_alive),
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

fn create_tce_config(
    secret_key: SecretKey,
    addresses: &Addresses,
) -> crate::Result<tashi_consensus_engine::Config> {
    let nodes = addresses
        .addresses
        .iter()
        .map(|address| (address.key.clone(), address.addr))
        .collect();

    Ok(tashi_consensus_engine::Config::new(secret_key)
        .initial_nodes(nodes)
        .use_alpn(true)
        .enable_hole_punching(false)
        // TODO: we can dispatch messages before they come to consensus
        // but we need to make sure we don't duplicate PUBLISHes.
        // .report_events_before_consensus(true)
        // Since a FoxMQ cluster is permissioned, don't kick failed nodes which may later recover.
        .fallen_behind_kick_seconds(None))
}

/// NOTE: uses blocking I/O internally.
fn read_secret_key(path: &Path) -> crate::Result<SecretKey> {
    // There's no benefit to using `tokio::fs` because it just does the blocking work on a background thread.
    let pem = std::fs::read(path).wrap_err_with(|| format!("error reading {}", path.display()))?;

    SecretKey::from_pem(&pem)
        .wrap_err_with(|| format!("error reading P-256 secret key from {}", path.display()))
}
