use clap::Parser;
use color_eyre::eyre::Context;

use tashi_message_queue::args::Args;
use tashi_message_queue::config::Config;
use tashi_message_queue::mqtt::broker::MqttBroker;
use tashi_message_queue::Result;

fn main() -> Result<()> {
    let args = Args::parse();

    tashi_message_queue::bootstrap(args.log)?;

    tracing::debug!("read args: {args:?}");

    // File and stdio aren't truly async in Tokio so we might as well do that before we even start the runtime
    let config =
        tashi_message_queue::config::read(&args.config_file).wrap_err("error reading config")?;

    let tce_config = tashi_message_queue::create_tce_config(&args, &config)
        .wrap_err("error initializing TCE config")?;

    async_main(args, config, tce_config)
}

// `#[tokio::main]` doesn't have to be attached to the actual `main()`, and it can accept args
#[tokio::main]
async fn async_main(
    args: Args,
    _config: Config,
    _tce_config: tashi_consensus_engine::Config,
) -> Result<()> {
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
