use color_eyre::eyre;
use color_eyre::eyre::WrapErr;
use foxmq_metrics::resources::ResourcesContext;
use foxmq_metrics::{
    map_join_error, panic_payload_to_str, rate_to_period, BrokerArgs, InfluxDbArgs,
};
use futures::future::OptionFuture;
use futures::FutureExt;
use influxdb2::models::data_point::DataPointBuilder;
use influxdb2::models::DataPoint;
use paho_mqtt::{AsyncClient, ConnectOptions, ConnectOptionsBuilder, Message};
use rand::distributions::{Alphanumeric, DistString, Standard};
use rand::Rng;
use std::cmp;
use std::pin::pin;
use std::sync::Arc;
use std::time::Duration;
use tokio::task::JoinSet;
use tokio::time;
use tokio::time::MissedTickBehavior;
use tokio_util::sync::CancellationToken;

/// Publisher side of metrics gathering: open many connections to a broker and publish messages.
#[derive(clap::Parser)]
struct Args {
    /// The index of which publisher instance this is.
    ///
    /// Used when writing data to InfluxDB and in the MQTT client ID.
    #[clap(long, env = "PUBLISHER_INDEX")]
    index: u32,

    /// The number of connections to open.
    #[clap(short, long, default_value = "1", env = "PUBLISHER_CONNECTIONS")]
    connections: u32,

    /// The rate at which to connect to the broker.
    #[clap(long, default_value = "1", env = "PUBLISHER_CONNECTION_RATE")]
    connection_rate: f64,

    /// The initial message rate, in messages per second, per connection. May be fractional.
    #[clap(long, env = "PUBLISHER_INITIAL_RATE")]
    initial_rate: f64,

    /// The maximum message rate, in messages per second, per connection. May be fractional.
    #[clap(long, env = "PUBLISHER_MAX_RATE")]
    max_rate: f64,

    /// The number to multiply the current rate by, after every `ramp_period`.
    #[clap(long, env = "PUBLISHER_RAMP_FACTOR")]
    ramp_factor: f64,

    /// The time, in seconds, between applications of `ramp_factor`.
    #[clap(long, env = "PUBLISHER_RAMP_PERIOD")]
    ramp_period: f64,

    /// The period at which to submit measurements to InfluxDB, in seconds.
    #[clap(long, env = "PUBLISHER_SAMPLE_PERIOD")]
    sample_period: f64,

    /// The length of topics to generate, in ASCII characters.
    ///
    /// Note that FoxMQ currently enforces a maximum topic length of 1024 characters.
    #[clap(long, env = "PUBLISHER_TOPIC_LENGTH")]
    topic_length: u16,

    /// The size of messages to generate, in bytes. Max size: 64 KiB.
    #[clap(long, env = "PUBLISHER_MESSAGE_SIZE")]
    message_size: u16,

    /// The Quality of Service (QoS) level to publish messages at.
    ///
    /// Valid values: 0, 1, 2
    ///
    /// 0: At Most Once delivery, messages may be dropped.
    /// 1: At Least Once delivery, messages may not be dropped but may be duplicated.
    /// 2: Exactly Once delivery, messages may not be dropped nor duplicated.
    #[clap(
        long,
        env = "PUBLISHER_PUBLISH_QOS",
        value_parser = clap::value_parser!(u8).range(0..=2)
        default_value = "1",
    )]
    publish_qos: u8,

    #[clap(flatten)]
    broker: BrokerArgs,

    #[clap(flatten)]
    influxdb: InfluxDbArgs,
}

struct Context {
    args: Args,
    influxdb: Option<influxdb2::Client>,
    token: CancellationToken,
}

struct ConnectionMeasurements {
    messages_sent: u32,
    bytes_sent: i64,
    rate: f64,
}

#[tokio::main]
async fn main() -> eyre::Result<()> {
    dotenvy::dotenv().ok();
    tracing_subscriber::fmt::init();

    let args: Args = clap::Parser::parse();
    let influxdb = args.influxdb.client();

    let mut resources = ResourcesContext::new().wrap_err("failed to create ResourcesContext")?;

    let mut connection_interval = time::interval(rate_to_period(args.connection_rate));
    // We don't want to burst as that could overwhelm the broker.
    connection_interval.set_missed_tick_behavior(MissedTickBehavior::Delay);

    let mut sample_interval = time::interval(Duration::from_secs_f64(args.sample_period));
    sample_interval.set_missed_tick_behavior(MissedTickBehavior::Delay);

    let mut tasks = JoinSet::new();

    let mut ctrl_c = pin!(tokio::signal::ctrl_c());

    let context = Arc::new(Context {
        args,
        influxdb,
        token: CancellationToken::new(),
    });

    loop {
        let do_connect = tasks.len() <= context.args.connections as usize;

        let sample_tick: OptionFuture<_> = context
            .influxdb
            .as_ref()
            .map(|influxdb| async {
                sample_interval.tick().await;
                influxdb
            })
            .into();

        tokio::select! {
            _ = connection_interval.tick(), if do_connect => {
                // Always fits in `u32` if `do_connect` is `true`.
                let index = tasks.len() as u32;
                let task = connection_task(context.clone(), index);

                tasks.spawn(async move {
                    (
                        index,
                        // Catch panics so we can report the connection index.
                        task.catch_unwind().await
                    )
                });
            }
            Some(influxdb) = sample_interval.tick() => {
                submit_measurements(
                    &context,
                    influxdb,
                    &mut resources,
                    tasks.len()
                )
                .await
                .wrap_err("failed to submit measurements")?;
            }
            Some(res) = tasks.join_next() => {
                match res {
                    Ok((index, Ok(Ok(())))) => {
                        eyre::bail!("connection {index} exited early without error");
                    }
                    Ok((index, Ok(Err(err)))) => {
                        return Err(err).wrap_err_with(|| format!("connection {index} returned an error"));
                    }
                    Ok((index, Err(panic))) => {
                        eyre::bail!("connection {index} panicked with message: {}", panic_payload_to_str(&panic));
                    }
                    Err(e) => {
                        // Unlikely unless cancelled because we catch panics inside.
                        return Err(map_join_error(e)).wrap_err("an unknown connection exited");
                    }
                }
            }
            _ = ctrl_c.as_mut() => {
                tracing::info!("Ctrl-C received; shutting down");
                break;
            }
        }
    }

    context.token.cancel();

    let mut log_interval = time::interval(Duration::from_secs(5));

    while !tasks.is_empty() {
        tokio::select! {
            _ = log_interval.tick() => {
                tracing::info!("waiting for {} connections to exit", tasks.len());
            }
            Some(res) = tasks.join_next() => {
                match res {
                    Ok((_, Ok(Ok(())))) => (),
                    Ok((index, Ok(Err(err)))) => {
                        return Err(err).wrap_err_with(|| format!("connection {index} returned an error"));
                    }
                    Ok((index, Err(panic))) => {
                        eyre::bail!("connection {index} panicked with message: {}", panic_payload_to_str(&panic));
                    }
                    Err(e) => {
                        // Unlikely unless cancelled because we catch panics inside.
                        return Err(map_join_error(e)).wrap_err("an unknown connection exited");
                    }
                }
            }
            _ = ctrl_c.as_mut() => {
                tracing::info!("Ctrl-C received again; exiting without closing {} remaining connections", tasks.len());
                break;
            }
        }
    }

    Ok(())
}

#[tracing::instrument(skip(context))]
async fn connection_task(context: Arc<Context>, index: u32) -> eyre::Result<()> {
    // MQTT client IDs can be up to 23 bytes. Split evenly, that gives us a max of 10 digits each
    // for publisher index and connection index in this string format.
    //
    // The max value for `u32` is 10 digits, so using `u32` for publisher and connection indices
    // guarantees that we can never generate a client ID that is too large.
    let client_id = format!("p{}c{index}", context.args.index);

    let client = context
        .args
        .broker
        .connect(&client_id)
        .await
        .wrap_err("error connecting to broker")?;

    let mut rate = context.args.initial_rate;

    let mut message_interval = time::interval(rate_to_period(rate));

    let mut sample_interval = time::interval(Duration::from_secs_f64(context.args.sample_period));

    let mut messages_sent = 0;
    let mut bytes_sent = 0;

    loop {
        let sample_tick: OptionFuture<_> = context
            .influxdb
            .as_ref()
            .map(|influxdb| async {
                sample_interval.tick().await;
                influxdb
            })
            .into();

        tokio::select! {
            _ = message_interval.tick() => {
                publish_message(&context, &client)
                    .await
                    .wrap_err("error publishing message to broker")?;

                bytes_sent += context.args.message_size as i64;

                messages_sent += 1;
            },
            Some(influxdb) = sample_tick => {
                submit_connection_measurements(
                    &context,
                    influxdb,
                    index,
                    ConnectionMeasurements {
                        messages_sent,
                        bytes_sent,
                        rate
                    }
                )
                .await
                .wrap_err("error submitting connection measurements")?;

                // Reset for the next sample.
                messages_sent = 0;
                bytes_sent = 0;
            }
            _ = context.token.cancelled() => {
                break;
            }
        }
    }

    client.disconnect(None).await?;

    Ok(())
}

async fn publish_message(context: &Context, client: &AsyncClient) -> eyre::Result<()> {
    let topic = rand_alphanumeric(context.args.topic_length as usize);
    let message = rand_bytes(context.args.message_size as usize);

    client
        .publish(Message::new(
            topic,
            message,
            context.args.publish_qos as i32,
        ))
        .await?;

    Ok(())
}

async fn submit_measurements(
    context: &Context,
    influxdb: &influxdb2::Client,
    resources: &mut ResourcesContext,
    connections: usize,
) -> eyre::Result<()> {
    let resources = resources
        .sample()
        .wrap_err("failed to sample resources")?
        .to_datapoint()
        .tag("publisher", context.args.index.to_string())
        .build()
        // This is the only reason for `.build()` to fail.
        .expect("BUG: DataPoint should not be empty");

    let publishers = DataPoint::builder("publishers")
        .tag("publisher", context.args.index.to_string())
        .field("connections", connections.to_string())
        .build()
        .expect("BUG: DataPoint should not be empty");

    influxdb
        .write(
            &context.args.influxdb.influxdb_bucket,
            futures::stream::iter([resources, publishers]),
        )
        .await
        .wrap_err("failed to submit measurements to InfluxDB")
}

async fn submit_connection_measurements(
    context: &Context,
    influxdb: &influxdb2::Client,
    connection_index: u32,
    measurements: ConnectionMeasurements,
) -> eyre::Result<()> {
    // Destructuring ensures that if any new fields are added, we don't forget to handle them.
    let ConnectionMeasurements {
        messages_sent,
        rate,
    } = measurements;

    let messages = DataPoint::builder("messages")
        .tag("publisher", context.args.index.to_string())
        .tag("connection", connection_index.to_string())
        .field("sent", messages_sent as i64)
        .field("rate", rate)
        .build()
        .expect("BUG: DataPoint should not be empty");

    influxdb
        .write(
            &context.args.influxdb.influxdb_bucket,
            futures::stream::iter([messages]),
        )
        .await
        .wrap_err("failed to submit measurement to InfluxDB")
}

fn apply_ramp(rate: f64, max_rate: f64, ramp_factor: f64) -> f64 {
    // Note: not `Ord::max` because `f64` doesn't implement `Ord`
    f64::max(rate * ramp_factor, max_rate)
}

fn rand_alphanumeric(len: usize) -> String {
    Alphanumeric.sample_string(&mut rand::thread_rng(), len)
}

fn rand_bytes(len: usize) -> Vec<u8> {
    // Surprisingly, `rand` doesn't give an easier way to do this.
    let mut bytes = vec![0u8; len];
    rand::thread_rng().fill(&mut bytes[..]);
    bytes
}
