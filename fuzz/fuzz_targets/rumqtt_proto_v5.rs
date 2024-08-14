#![no_main]

use std::hint::black_box;

use bytes::BytesMut;
use libfuzzer_sys::fuzz_target;

use rumqttd_protocol::v5::V5;
use rumqttd_protocol::Protocol;

fuzz_target!(|data: &[u8]| {
    let max = data.len();
    let mut bytes = BytesMut::from(data);

    let mut output = Vec::new();

    if let Ok(res) = black_box(V5.read_mut(&mut bytes, max)) {
        V5.write(res, &mut output).unwrap();
    }
});
