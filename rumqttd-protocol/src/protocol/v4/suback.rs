use super::*;
use bytes::{Buf, BufMut, Bytes, BytesMut};
use std::convert::{TryFrom, TryInto};

fn len(suback: &SubAck) -> usize {
    2 + suback.return_codes.len()
}

pub fn read(fixed_header: FixedHeader, mut bytes: Bytes) -> Result<SubAck, Error> {
    let variable_header_index = fixed_header.fixed_header_len;
    bytes.advance(variable_header_index);
    let pkid = read_u16(&mut bytes)?;

    if !bytes.has_remaining() {
        return Err(Error::MalformedPacket);
    }

    let mut return_codes = Vec::new();
    while bytes.has_remaining() {
        let return_code = read_u8(&mut bytes)?;
        return_codes.push(reason(return_code)?);
    }

    let suback = SubAck { pkid, return_codes };
    Ok(suback)
}

pub fn write(suback: &SubAck, buffer: &mut Vec<u8>) -> Result<usize, Error> {
    let remaining_len = suback.len();
    reserve_buffer(buffer, remaining_len);

    buffer.put_u8(0x90);
    let remaining_len_bytes = write_remaining_length(buffer, remaining_len)?;

    buffer.put_u16(suback.pkid);
    let p: Vec<u8> = suback.return_codes.iter().map(|&c| code(c)).collect();

    buffer.extend_from_slice(&p);
    Ok(1 + remaining_len_bytes + remaining_len)
}

fn reason(code: u8) -> Result<SubscribeReasonCode, Error> {
    let v = match code {
        0 => SubscribeReasonCode::Success(QoS::AtMostOnce),
        1 => SubscribeReasonCode::Success(QoS::AtLeastOnce),
        2 => SubscribeReasonCode::Success(QoS::ExactlyOnce),
        0x80 => SubscribeReasonCode::Failure,
        v => return Err(Error::InvalidSubscribeReasonCode(v)),
    };

    Ok(v)
}

fn code(reason: SubscribeReasonCode) -> u8 {
    match reason {
        SubscribeReasonCode::Success(qos) => qos as u8,
        // SUBACK return codes other than 0x00, 0x01, 0x02 and 0x80 are reserved and MUST NOT be used [MQTT-3.9.3-2].
        _ => 0x80,
    }
}
