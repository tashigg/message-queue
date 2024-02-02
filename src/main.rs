use bytes::BytesMut;
use clap::Parser;
use color_eyre::eyre::WrapErr;
use std::net::SocketAddr;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use tracing::Instrument;

use message_queue::args::Args;
use message_queue::Result;

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    message_queue::bootstrap(args.log)?;

    let mut listener = TcpListener::bind(args.listen_addr)
        .await
        .wrap_err_with(|| format!("failed to bind listen_addr: {}", args.listen_addr))?;

    let mut connection_tasks = JoinSet::new();

    let exit_token = CancellationToken::new();

    tracing::info!(listen_addr = %args.listen_addr, "listening for connections");

    loop {
        tokio::select! {
            res = listener.accept() => {
                let (conn, remote_addr) = res.wrap_err("error from socket")?;

                tracing::info!(%remote_addr, "connection received");

                connection_tasks.spawn(
                    run_connection(remote_addr, conn, exit_token.clone())
                );
            }
            Some(_) = connection_tasks.join_next() => {
                // Errors are logged with `#[tracing::instrument]`
            }
            res = tokio::signal::ctrl_c() => {
                res.wrap_err("error from Ctrl-C handler")?;

                exit_token.cancel();
                break;
            }
        }
    }

    tracing::info!(
        "Ctrl-C received; waiting for {} connections to close",
        connection_tasks.len()
    );

    while let Some(_) = connection_tasks.join_next().await {
        tracing::info!("{} connections remaining", connection_tasks.len());
    }

    Ok(())
}

#[tracing::instrument(skip_all, fields(remote_addr=%_remote_addr), err)]
async fn run_connection(
    _remote_addr: SocketAddr,
    mut conn: TcpStream,
    exit_token: CancellationToken,
) -> Result<()> {
    let mut buffer = BytesMut::with_capacity(8192);

    while !exit_token.is_cancelled() {
        tokio::select! {
            res = conn.read_buf(&mut buffer) => {
                let read = res?;

                if read == 0 {
                    tracing::debug!("connection closed by remote peer");
                    break;
                }

                tracing::debug!("read {read} bytes from connection");
            }
            _ = exit_token.cancelled() => {
                break;
            }
        }
    }

    // TODO: send `DISCONNECT`
    Ok(conn.shutdown().await?)
}
