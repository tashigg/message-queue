use clap::Parser;
use tokio::io::AsyncWriteExt;

use tashi_message_queue::args::Args;
use tashi_message_queue::mqtt::broker::MqttBroker;
use tashi_message_queue::Result;

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    tashi_message_queue::bootstrap(args.log)?;

    let mut broker = MqttBroker::bind(args.listen_addr).await?;

    loop {
        tokio::select! {
            res = broker.run() => {
                res?;
            }

            res = tokio::signal::ctrl_c() => {
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
