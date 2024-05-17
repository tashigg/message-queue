use color_eyre::eyre;
use color_eyre::eyre::{Error, WrapErr};
use paho_mqtt::{AsyncClient, ConnectOptionsBuilder};
use std::any::Any;
use std::time::Duration;

pub mod resources;

#[derive(Debug, clap::Args)]
pub struct InfluxDbArgs {
    /// The URL of the InfluxDB v2 API to report measurements to.
    #[clap(
        long,
        env = "INFLUXDB_URL",
        requires = "influxdb_org",
        requires = "influxdb_key"
    )]
    pub influxdb_url: Option<String>,

    #[clap(
        long,
        env = "INFLUXDB_ORG",
        requires = "influxdb_url",
        requires = "influxdb_key"
    )]
    pub influxdb_org: Option<String>,

    #[clap(
        long,
        env = "INFLUXDB_KEY",
        requires = "influxdb_org",
        requires = "influxdb_key"
    )]
    pub influxdb_key: Option<String>,

    #[clap(long, env = "INFLUXDB_BUCKET", default_value = "foxmq-metrics")]
    pub influxdb_bucket: String,
}

#[derive(Debug, clap::Args)]
pub struct BrokerArgs {
    #[clap(long, env = "BROKER_URL")]
    pub broker_url: String,

    #[clap(long, env = "BROKER_USERNAME")]
    pub broker_username: Option<String>,

    #[clap(long, env = "BROKER_PASSWORD")]
    pub broker_password: Option<String>,
}

impl InfluxDbArgs {
    pub fn client(&self) -> Option<influxdb2::Client> {
        // Note: `ClientBuilder::build()` only errors if the `reqwest::Client` fails to build;
        // it doesn't check any of these parameters, so it's all that useful.
        Some(influxdb2::Client::new(
            self.influxdb_url.as_deref()?,
            self.influxdb_org.as_deref()?,
            self.influxdb_key.as_deref()?,
        ))
    }
}

impl BrokerArgs {
    pub async fn connect(&self, client_id: &str) -> eyre::Result<AsyncClient> {
        let client = AsyncClient::new((&self.broker_url[..], &client_id[..]))
            .wrap_err("failed to create client")?;

        let mut opts_builder = ConnectOptionsBuilder::new_v5();
        // We never want to resume a session as that could corrupt the statistics.
        opts_builder.clean_start(true);

        if let Some(username) = &self.broker_username {
            opts_builder.user_name(username);
        }

        if let Some(password) = &self.broker_password {
            opts_builder.password(&password);
        }

        let response = client
            .connect(opts_builder.finalize())
            .await
            .wrap_err("failed to connect to broker")?;

        if response.reason_code().is_err() {
            eyre::bail!("broker returned reason code: {:?}", response.reason_code());
        }

        Ok(client)
    }
}

/// Convert a rate, in events per second, to a duration between events.
pub fn rate_to_period(rate: f64) -> Duration {
    Duration::from_secs_f64(rate.recip())
}

// Don't want to build all of FoxMQ for these functions.
pub fn map_join_error(err: tokio::task::JoinError) -> Error {
    let Ok(panic) = err.try_into_panic() else {
        return eyre::eyre!("task cancelled");
    };

    let panic_str = panic_payload_to_str(&panic);

    eyre::eyre!("task panicked: {panic_str}")
}

/// Extract a string from a panic payload.
pub fn panic_payload_to_str<'a>(panic: &'a (dyn Any + 'static)) -> &'a str {
    // Panic payloads are almost always `String` (if made with formatting arguments)
    // or `&'static str` (if given a string literal).
    //
    // Non-string payloads are a legacy feature so we don't need to worry about those much.
    //
    // It's really annoying that there isn't something like this in the stdlib already,
    // as I've had to write pretty much this exact code at least a dozen times.
    panic
        .downcast_ref::<String>()
        .map(|s| &**s)
        .or_else(|| panic.downcast_ref::<&'static str>().copied())
        .unwrap_or("(non-string payload)")
}
