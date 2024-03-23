#![no_main]

use std::hint::black_box;

use libfuzzer_sys::fuzz_target;

use tashi_message_queue::mqtt::client_id::ClientId;

fuzz_target!(|data: &[u8]| {
    if let Ok(client_id) = ClientId::from_bytes(data) {
        // With `cfg(debug_assertions)` set,
        // this will assert that the string is valid UTF-8 in the given length range.
        black_box(client_id.as_str());
    }
});
