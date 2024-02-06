use color_eyre::eyre::WrapErr;
use std::net::SocketAddr;
use std::path::PathBuf;
use tashi_consensus_engine::SecretKey;

#[derive(clap::Parser, Debug)]
pub struct Args {
    #[clap(short, long, default_value = "full")]
    pub log: LogFormat,

    #[clap(short = 'L', long, default_value = "0.0.0.0:1883")]
    pub mqtt_listen_addr: SocketAddr,

    #[clap(short = 'T', long, default_value = "0.0.0.0:49213")]
    pub tce_listen_addr: SocketAddr,

    #[command(flatten)]
    pub secret_key: SecretKeyOpt,

    pub config_file: PathBuf,
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

#[derive(clap::ValueEnum, Debug, Copy, Clone)]
pub enum LogFormat {
    Full,
    Compact,
    Pretty,
    Json,
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
