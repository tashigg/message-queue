#![no_main]

use std::hint::black_box;

use bytes::BytesMut;
use libfuzzer_sys::fuzz_target;

use rumqttd_shim::protocol::v4::V4;
use rumqttd_shim::protocol::Protocol;

fuzz_target!(|data: &[u8]| {
    let max = data.len();
    let mut bytes = BytesMut::from(data);

    let _ = black_box(V4.read_mut(&mut bytes, max));
});
