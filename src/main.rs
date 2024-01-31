use clap::Parser;

use message_queue::args::Args;
use message_queue::Result;

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    message_queue::bootstrap(args.log)?;

    // Application main loop here

    Ok(())
}
