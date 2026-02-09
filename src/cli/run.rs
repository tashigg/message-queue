use crate::cli::LogFormat;
use crate::config;
use crate::config::addresses::Addresses;
use crate::config::permissions::PermissionsConfig;
use crate::config::users::{AuthConfig, UsersConfig};
use crate::mqtt::broker::{self, MqttBroker};
use crate::mqtt::{KeepAlive, TceState};
// use crate::transaction::AddNodeTransaction;
use color_eyre::eyre;
use color_eyre::eyre::Context;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::Arc;
use tashi_vertex::{KeyPublic, KeySecret, Options};
use tokio_rustls::rustls;

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

    #[command(flatten)]
    pub ws_config: WsConfig,

    #[command(flatten)]
    pub cluster_config: ClusterConfig,

    /// The directory containing `address-book.toml` and (optionally) `users.toml`.
    #[clap(default_value = "foxmq.d/")]
    pub config_dir: PathBuf,
}

#[derive(clap::Args, Debug, Clone)]
#[group(required = false, multiple = false)]
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
    /// Note: this only applies to MQTT-over-TLS connections (`--mqtts`).
    ///
    /// Defaults to the main secret key (`--secret-key`/`--secret-key-file`).
    #[clap(long)]
    pub tls_key_file: Option<PathBuf>,

    /// Path to the X.509 certificate to use for TLS.
    ///
    /// Note: this only applies to MQTT-over-TLS connections  (`--mqtts`).
    ///
    /// Defaults to a certificate self-signed with either `--tls-key-file`
    /// or the main secret key (`--secret-key`/`--secret-key-file`).
    #[clap(long)]
    pub tls_cert_file: Option<PathBuf>,
}

/// Websockets configuration
#[derive(clap::Args, Debug, Clone)]
pub struct WsConfig {
    /// Enable listening for MQTT-over-Websockets connections on a separate socket (0.0.0.0:8080 by default).
    #[clap(long)]
    pub websockets: bool,

    /// The TCP socket address to listen for MQTT-over-Websockets (`ws`) connections from clients.
    #[clap(long, default_value = "0.0.0.0:8080")]
    pub websockets_addr: SocketAddr,
}

#[derive(clap::Args, Debug, Clone)]
pub struct ClusterConfig {
    /// Path to a TLS certificate or certificate chain file
    /// to present to peers on new cluster connections.
    ///
    /// If not set, a self-signed TLS certificate is generated on startup.
    ///
    /// If set, the certificate _must_ match the secret key specified by `--secret-key`
    /// or `--secret-key-path`.
    #[clap(long, env)]
    pub cluster_cert: Option<PathBuf>,

    /// Path to a TLS root certificate file to use for cluster operations.
    ///
    /// If set, all peers must present a valid TLS certificate chain that
    /// ends with this certificate.
    #[clap(long, env, requires("cluster_cert"))]
    pub cluster_root_cert: Option<PathBuf>,

    /// If set, admit any peer to the cluster that connects with a valid TLS certificate.
    ///
    /// This allows peers to connect even if they aren't in the initial address book.
    ///
    /// A valid certificate is any certificate chain that ends with the certificate
    /// specified by `--cluster-root-cert`.
    #[clap(long, env, requires("cluster_root_cert"))]
    pub cluster_accept_peer_with_cert: bool,
}

trait KeySecretToRustls {
    fn to_rustls(&self) -> crate::Result<rustls::pki_types::PrivateKeyDer<'static>>;
}

impl KeySecretToRustls for KeySecret {
    fn to_rustls(&self) -> crate::Result<rustls::pki_types::PrivateKeyDer<'static>> {
        let der = self.to_der_vec().wrap_err("failed to encode key to DER")?;
        Ok(rustls::pki_types::PrivateKeyDer::Pkcs8(der.into()))
    }
}

struct TceConfig {
    options: Options,
    pub secret_key: KeySecret,
    pub _joining_running_session: bool,
    pub initial_peers: Vec<(KeyPublic, SocketAddr)>,
}

impl SecretKeyOpt {
    /// NOTE: uses blocking I/O internally if the secret key was specified as a file.
    pub fn read_key(&self) -> crate::Result<KeySecret> {
        if let Some(key_input) = &self.secret_key {
            // First, try to parse directly as a Base58 secret key
            if let Ok(key) = KeySecret::from_str(key_input) {
                return Ok(key);
            }

            // If that fails, treat it as a file path
            let content = std::fs::read_to_string(key_input)
                .wrap_err_with(|| format!("failed to read secret key file: {key_input}"))?;

            return KeySecret::from_str(content.trim())
                .wrap_err("failed to parse secret key from file");
        }

        if let Some(path) = &self.secret_key_file {
            return read_secret_key(path);
        }

        Ok(KeySecret::generate())
    }
}

pub fn main(args: RunArgs) -> crate::Result<()> {
    let mut users = config::users::read(&args.config_dir.join("users.toml"))?;
    let acl = config::permissions::read(&args.config_dir.join("permissions.toml"))?;

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

    eyre::ensure!(!users.by_username.is_empty() || users.auth.allow_anonymous_login);

    let secret_key = args.secret_key.read_key()?;

    // File and stdio aren't truly async in Tokio so we might as well do that before we even start the runtime
    let tce_config = match config::addresses::read(&args.config_dir.join("address-book.toml")) {
        Ok(addresses) => {
            let tce_config =
                create_tce_config(secret_key.clone(), &addresses, &args.cluster_config)
                    .wrap_err("error initializing TCE config")?;

            Some(tce_config)
        }
        Err(_) => {
            tracing::info!("Running in non-clustered mode");
            None
        }
    };

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

                rustls_pemfile::certs(&mut &cert_pem[..])
                    .collect::<Result<Vec<_>, _>>()
                    .wrap_err_with(|| {
                        format!(
                            "error reading certificate chain from {}",
                            cert_file.display()
                        )
                    })?
            } else {
                // Certificate::generate_self_signed was from TCE.
                // We need to implement using rcgen or similar if needed, or require cert file.
                eyre::bail!("Self-signed certificate generation is temporarily disabled during migration. Please provide a certificate file.");
            };

            eyre::Ok(broker::TlsConfig {
                socket_addr: tls_socket_addr,
                cert_chain,
                key: key.to_rustls()?,
            })
        })
        .transpose()?;

    let ws_config = args.ws_config.websockets.then(|| args.ws_config.clone());

    main_async(args, users, acl, tce_config, tls_config, ws_config)
}

// `#[tokio::main]` doesn't have to be attached to the actual `main()`, and it can accept args
#[tokio::main]
async fn main_async(
    args: RunArgs,
    users: UsersConfig,
    permissions_config: PermissionsConfig,
    tce_config: Option<TceConfig>,
    tls_config: Option<broker::TlsConfig>,
    ws_config: Option<WsConfig>,
) -> crate::Result<()> {
    let tce = match tce_config {
        Some(tce_config) => {
            let context =
                Arc::new(tashi_vertex::Context::new().wrap_err("failed to create context")?);
            let socket = tashi_vertex::Socket::bind(&context, &args.cluster_addr.to_string())
                .await
                .wrap_err("failed to bind cluster socket")?;
            let mut peers = tashi_vertex::Peers::new().wrap_err("failed to create peers")?;

            for (key, addr) in tce_config.initial_peers {
                peers
                    .insert(
                        &addr.to_string(),
                        &key,
                        tashi_vertex::PeerCapabilities::default(),
                    )
                    .wrap_err("failed to add initial peer")?;
            }

            let engine = tashi_vertex::Engine::start(
                &context,
                socket,
                tce_config.options,
                &tce_config.secret_key,
                peers,
            )
            .wrap_err("failed to start engine")?;

            Some(TceState {
                engine: Arc::new(engine),
                context,
            })
        }
        None => None,
    };

    let tce_platform = tce.as_ref().map(|tce| tce.engine.clone());

    let mut broker = MqttBroker::bind(
        args.mqtt_addr,
        tls_config,
        ws_config,
        users,
        permissions_config,
        tce,
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

                if let Some(_platform) = tce_platform {
                    // platform.shutdown().await;
                }
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
    secret_key: KeySecret,
    addresses: &Addresses,
    config: &ClusterConfig,
) -> crate::Result<TceConfig> {
    // let nodes: HashMap<_, _> = addresses
    //     .addresses
    //     .iter()
    //     .map(|address| (address.key.clone(), address.addr))
    //     .collect();

    // The address book is only required to contain the existing nodes.
    // let joining_running_session = !nodes.contains_key(&secret_key.public());
    // For now assume we are always joining or starting based on some other logic, or just let vertex handle it?
    // tashi-vertex handle this via initial peers.
    let _joining_running_session = false; // TODO: restore logic

    let options = Options::new();

    // Add initial peers
    let mut initial_peers = Vec::new();
    for address in &addresses.addresses {
        let addr: SocketAddr = address
            .addr
            .to_string()
            .parse()
            .expect("invalid socket address");
        initial_peers.push((address.key.clone(), addr));
    }

    if let Some(_cert_path) = &config.cluster_cert {
        // TODO: handle certs in tashi-vertex
        // options.tls_cert_chain(Certificate::load_chain_from(cert_path)?);
    }

    /*
    let roots = if let Some(root_cert_path) = &config.cluster_root_cert {
        let roots = Arc::new(RootCertificates::read_from(root_cert_path)?);
        tce_config.tls_roots(roots.clone());
        Some(roots)
    } else {
        None
    };
    */

    /*
    let (add_nodes_tx, add_nodes_rx) = mpsc::unbounded_channel();

    if config.cluster_accept_peer_with_cert {
        tce_config.on_unknown_connection(move |addr, key, certs| {
            // Certificate chain has already been verified by TCE at this point.

            add_nodes_tx
                .send(AddNodeTransaction {
                    socket_addr: addr.into(),
                    key: key.clone(),
                    certs: certs.iter().map(Into::into).collect(),
                })
                .ok();

            Ok(UnknownConnectionAction::VoteToAddPeer)
        });
    }
    */

    Ok(TceConfig {
        options,
        secret_key: secret_key.clone(),
        _joining_running_session,
        initial_peers,
    })
}

/// NOTE: uses blocking I/O internally.
fn read_secret_key(path: &Path) -> crate::Result<KeySecret> {
    // There's no benefit to using `tokio::fs` because it just does the blocking work on a background thread.
    let pem = std::fs::read(path).wrap_err_with(|| format!("error reading {}", path.display()))?;

    KeySecret::from_der(&pem)
        .wrap_err_with(|| format!("error reading P-256 secret key from {}", path.display()))
}
