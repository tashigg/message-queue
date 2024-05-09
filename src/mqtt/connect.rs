//! Unified (multi-version) CONNECT packet parsing.
//!
//! Rumqttd's protocol code has completely different routines for parsing V4 and V5 connect packets,
//! and the parsing functions consume data from the `&mut BytesMut` passed, making it annoying
//! to handle multiple versions of the protocol in the same code path.
//!
//! Rumqttd just has clients using different versions connect to different endpoints,
//! which seems worse all round.
//!
//! Worse still, the v5 code seems to be a wholesale copy of the v4 code, with many completely
//! identical functions (e.g. `check()`, `FixedHeader` and its methods, etc.).

use crate::mqtt::{protocol, DynProtocol};
use bytes::BytesMut;
use rumqttd_protocol::v4::V4;
use rumqttd_protocol::v5::V5;
use rumqttd_protocol::{
    Connect, ConnectProperties, LastWill, LastWillProperties, Login, Packet, Protocol,
};

#[derive(Debug)]
pub struct ConnectPacket {
    pub protocol: DynProtocol,
    pub connect: Connect,
    pub connect_props: Option<ConnectProperties>,
    pub last_will: Option<LastWill>,
    pub last_will_props: Option<LastWillProperties>,
    pub login: Option<Login>,
}

/// Read a MQTT V4 (3.1.1) or V5 (5.0) CONNECT packet from the front of `stream`.
pub fn read(stream: &mut BytesMut, max_size: usize) -> Result<ConnectPacket, protocol::Error> {
    // Detect the protocol version and use that version's parsing routine.

    // This routine is identical in `protocol::v4`, not sure why it was copied.
    let fixed_header = protocol::v5::check(stream.iter(), max_size)?;

    let version_string = stream
        .get(
            // Get the variable header
            fixed_header.fixed_header_len()..,
        )
        .expect("BUG: v5::check() should have checked that we have a whole packet")
        // The variable header of the CONNECT packet starts with two fields identifying the protocol:
        // * Protocol name: 6 bytes
        //     * Length MSB (0)
        //     * Length LSB (4)
        //     * String data: 'MQTT'
        // * Protocol version: 1 byte
        .first_chunk::<7>()
        .ok_or(protocol::Error::MalformedPacket)?;

    let (protocol, packet) = match version_string {
        // MQTT version 5
        b"\x00\x04MQTT\x05" => (DynProtocol::V5, V5.read_mut(stream, max_size)?),
        // Otherwise assume version 4 since that's all we have currently.
        // Most clients appear to default to version 4 unless 5 is specifically requested.
        _ => (DynProtocol::V4, V4.read_mut(stream, max_size)?),
    };

    let Packet::Connect(connect, connect_props, last_will, last_will_props, login) = packet else {
        return Err(protocol::Error::MalformedPacket);
    };

    Ok(ConnectPacket {
        protocol,
        connect,
        connect_props,
        last_will,
        last_will_props,
        login,
    })
}
