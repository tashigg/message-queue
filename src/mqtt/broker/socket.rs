use std::future::Future;
use std::net::SocketAddr;

use bytes::BytesMut;
use color_eyre::eyre;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

pub trait MqttSocket: Send + Unpin + 'static {
    fn remote_addr(&self) -> SocketAddr;

    /// Read into `buf`.
    ///
    /// The capacity of `buf` should be greater than or equal to the expected packet size.
    ///
    /// Whether the read is allowed to exceed the capacity of `buf` or not is implementation-defined.
    fn read(&mut self, buf: &mut BytesMut) -> impl Future<Output = eyre::Result<usize>> + Send;

    /// Write the contents of `buf` to the socket and clear it.
    ///
    /// `buf` may or may not retain its original capacity.
    fn write_take_all(
        &mut self,
        buf: &mut Vec<u8>,
    ) -> impl Future<Output = eyre::Result<()>> + Send;

    fn shutdown(&mut self) -> impl Future<Output = eyre::Result<()>> + Send;
}

pub struct DirectSocket<S> {
    remote_addr: SocketAddr,
    stream: S,
}

impl<S> DirectSocket<S> {
    pub fn new(remote_addr: SocketAddr, stream: S) -> Self {
        Self {
            remote_addr,
            stream,
        }
    }
}

impl<S: AsyncRead + AsyncWrite + Send + Unpin + 'static> MqttSocket for DirectSocket<S> {
    fn remote_addr(&self) -> SocketAddr {
        self.remote_addr
    }

    async fn read(&mut self, buf: &mut BytesMut) -> eyre::Result<usize> {
        Ok(self.stream.read_buf(buf).await?)
    }

    async fn write_take_all(&mut self, buf: &mut Vec<u8>) -> eyre::Result<()> {
        self.stream.write_all(buf).await?;
        buf.clear();
        Ok(())
    }

    async fn shutdown(&mut self) -> eyre::Result<()> {
        Ok(self.stream.shutdown().await?)
    }
}
