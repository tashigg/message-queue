use std::net::SocketAddr;

#[derive(clap::Parser, Debug)]
pub struct Args {
    #[clap(short, long, default_value = "full")]
    pub log: LogFormat,

    #[clap(short = 'L', long, default_value = "0.0.0.0:1883")]
    pub listen_addr: SocketAddr,
}

#[derive(clap::ValueEnum, Debug, Copy, Clone)]
pub enum LogFormat {
    Full,
    Compact,
    Pretty,
    Json,
}
