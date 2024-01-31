#[derive(clap::Parser, Debug)]
pub struct Args {
    #[clap(short, long, default_value = "full")]
    pub log: LogFormat,
}

#[derive(clap::ValueEnum, Debug, Copy, Clone)]
pub enum LogFormat {
    Full,
    Compact,
    Pretty,
    Json,
}
