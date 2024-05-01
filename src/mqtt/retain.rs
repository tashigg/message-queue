use crate::mqtt::trie::Filter;
use crate::tce_message::PublishTrasaction;
use bytes::{Buf, BufMut, BytesMut};
use color_eyre::eyre;
use color_eyre::eyre::{ContextCompat, WrapErr};
use der::{Decode, Reader};
use slotmap::SlotMap;
use std::collections::{btree_map, BTreeMap};
use std::mem;
use std::ops::Bound;
use std::sync::Arc;
use tashi_consensus_engine::Timestamp;

slotmap::new_key_type! { struct MessageIndex; }

type TransactionIndex = usize;

#[derive(Default)]
pub struct RetainedMessages {
    messages: SlotMap<MessageIndex, Message>,
    by_timestamp: BTreeMap<(Timestamp, TransactionIndex), MessageIndex>,
    by_topic: BTreeMap<String, MessageIndex>,
}

struct Message {
    timestamp: Timestamp,
    transaction: TransactionIndex,
    publish: Arc<PublishTrasaction>,
}

impl RetainedMessages {
    pub const TAG: u8 = 0;

    pub fn len(&self) -> usize {
        self.messages.len()
    }

    pub fn insert(
        &mut self,
        timestamp: Timestamp,
        transaction: TransactionIndex,
        publish: impl Into<Arc<PublishTrasaction>>,
    ) {
        let message = Message {
            timestamp,
            transaction,
            publish: publish.into(),
        };

        // `.clone()` is unavoidable unless we want to do two map lookups.
        match self.by_topic.entry(message.publish.topic.clone()) {
            btree_map::Entry::Occupied(occupied) => {
                let index = *occupied.get();
                let replaced = mem::replace(&mut self.messages[index], message);
                self.by_timestamp
                    .remove(&(replaced.timestamp, replaced.transaction));
            }
            btree_map::Entry::Vacant(vacant) => {
                let index = self.messages.insert_with_key(|index| {
                    self.by_timestamp
                        .insert((message.timestamp, message.transaction), index);
                    message
                });
                vacant.insert(index);
            }
        }
    }

    pub fn remove(&mut self, topic: &str) {
        let Some(index) = self.by_topic.remove(topic) else {
            return;
        };

        let Some(removed) = self.messages.remove(index) else {
            return;
        };

        self.by_timestamp
            .remove(&(removed.timestamp, removed.transaction));
    }

    pub fn visit_matches(
        &self,
        topic_filter: &Filter,
        mut visit: impl FnMut(Timestamp, TransactionIndex, &Arc<PublishTrasaction>),
    ) {
        let mut visit_message = |message_id: MessageIndex| {
            let message = &self.messages[message_id];
            visit(message.timestamp, message.transaction, &message.publish);
        };

        let filter_prefix = match topic_filter.exact_or_prefix() {
            Ok(exact) => {
                if let Some(&message_id) = self.by_topic.get(exact) {
                    visit_message(message_id);
                }

                // An exact filter will only ever match one topic.
                // Regardless of whether we got a hit, there's no point in searching further.
                return;
            }
            // The topic filter contains at least one wildcard, so we need to search.
            //
            // First, find the right place to start in the BTreeMap.
            // This will be the wildcard-free prefix of the filter.
            Err(prefix) => prefix,
        };

        // A BTreeMap sorted by topics may return messages in different orders than they were sent;
        // this doesn't matter for Retain handling, however, as MQTT only guarantees ordering
        // for QoS 1 and 2 messages sent by the same client on the same topic.
        // There is only ever at most one retained message per topic.
        for (topic, &message_id) in self
            .by_topic
            // This is the shenanigans required to get a `.range()` call to compile
            // on a `BTreeMap<String, _>`.
            .range::<str, _>((Bound::Included(filter_prefix), Bound::Unbounded))
        {
            if topic_filter.matches_topic(topic) {
                visit_message(message_id);
            } else if !topic.starts_with(filter_prefix) {
                // Since `BTreeMap` will sort all topics with the same prefix together,
                // we can stop iterating once we don't have a common prefix anymore.
                break;
            }
        }
    }

    /// Serialization Format:
    ///
    /// ```ignore
    /// <Self::TAG><segment length in bytes: u64LE>[<timestamp: u64LE><index: u64LE><PublishTransaction DER> ..]
    /// ```
    ///
    /// Segment length includes the tag and the length itself.
    #[allow(dead_code)]
    pub fn serialize(&self, buf: &mut BytesMut) -> crate::Result<()> {
        let start = buf.len();

        buf.put_u8(Self::TAG);

        // Reserve space for our segment length
        let len_index = buf.len();
        buf.put_u64_le(0);

        for (&(timestamp, transaction), &index) in &self.by_timestamp {
            buf.put_u64_le(timestamp);
            buf.put_u64_le(transaction as u64);

            let publish = &*self.messages[index].publish;

            der::Encode::encode(publish, &mut buf.writer()).wrap_err_with(|| {
                format!("failed to encode transaction {timestamp}:{transaction}: {publish:?}")
            })?;
        }

        // Write the segment length
        let segment_len = buf.len() - start;
        (&mut buf[len_index..]).put_u64_le(segment_len as u64);

        Ok(())
    }

    /// Deserialize from the serialization format.
    ///
    /// The tag and segment length must be at the start of `buf`.
    #[allow(dead_code)]
    pub fn deserialize(mut buf: &[u8]) -> crate::Result<Self> {
        eyre::ensure!(
            !buf.is_empty(),
            "attempting to deserialize from an empty buffer"
        );

        let tag = buf.get_u8();
        eyre::ensure!(tag == Self::TAG, "expected tag {}, got {tag}", Self::TAG);

        eyre::ensure!(
            buf.len() >= 8,
            "expected at least 8 bytes for segment length, got {}",
            buf.len()
        );

        let segment_len = buf.get_u64_le();
        eyre::ensure!(
            segment_len >= 9,
            "expected at least 9 bytes for segment length, got {}",
            segment_len
        );

        let data_len = segment_len - 9;

        let data_len: usize = (segment_len - 9)
            .try_into()
            .wrap_err_with(|| format!("data_len overflows `usize`: {data_len}"))?;

        let mut buf = buf.get(..data_len).wrap_err_with(|| {
            format!(
                "expected buffer length of at least {data_len}, got {}",
                buf.len()
            )
        })?;

        let mut out = RetainedMessages::default();

        while !buf.is_empty() {
            let message = read_message(&mut buf)
                .wrap_err_with(|| format!("error reading {}th retained message", out.len()))?;

            out.insert(message.timestamp, message.transaction, message.publish);
        }

        Ok(out)
    }
}

fn read_message(buf: &mut &[u8]) -> crate::Result<Message> {
    eyre::ensure!(
        buf.len() >= 8,
        "expected at least 8 bytes for timestamp, got {}",
        buf.len()
    );

    let timestamp = buf.get_u64_le();

    eyre::ensure!(
        buf.len() >= 8,
        "expected at least 8 bytes for transaction index, got {}",
        buf.len()
    );

    let transaction = buf.get_u64_le();

    let transaction: usize = transaction
        .try_into()
        .wrap_err_with(|| format!("transaction index overflows `usize`: {transaction}"))?;

    let mut reader = der::SliceReader::new(
        usize::try_from(der::Length::MAX)
            .ok()
            .and_then(|max| buf.get(..max))
            .unwrap_or(*buf),
    )
    .expect("ensured `buf` is not longer than `der::Length::MAX`");

    let publish =
        PublishTrasaction::decode(&mut reader).wrap_err("error decoding PublishTransaction")?;

    // `pos` should be a valid `usize` pointing past the message in `buf`.
    let pos = u32::from(reader.position()) as usize;
    *buf = &buf[pos..];

    Ok(Message {
        timestamp,
        transaction,
        publish: publish.into(),
    })
}

#[cfg(test)]
mod tests {
    use super::RetainedMessages;
    use crate::tce_message::{
        BytesAsOctetString, PublishMeta, PublishTrasaction, TimestampSeconds,
    };
    use bytes::BytesMut;
    use rumqttd_protocol::QoS;
    use std::sync::Arc;

    #[test]
    fn serialize_round_trip() {
        let empty = RetainedMessages::default();

        let mut buf = BytesMut::new();
        empty.serialize(&mut buf).unwrap();

        #[rustfmt::skip]
        assert_eq!(
            *buf,
            [
                // Tag
                RetainedMessages::TAG,
                // Segment Length
                9, 0, 0, 0, 0, 0, 0, 0
            ]
        );

        let deserialized = RetainedMessages::deserialize(&buf).unwrap();
        assert_eq!(deserialized.len(), 0);

        let mut one_message = RetainedMessages::default();

        let publish = Arc::new(PublishTrasaction {
            topic: "test/message".to_string(),
            meta: PublishMeta::new(QoS::AtMostOnce, false, false),
            payload: BytesAsOctetString("Hello, world!".into()),
            timestamp_received: TimestampSeconds(12345678),
            properties: None,
        });

        one_message.insert(12345678, 0, publish.clone());

        let mut buf = BytesMut::new();
        one_message.serialize(&mut buf).unwrap();

        expect_test::expect![
            // `.assert_debug_eq()` adds a trailing newline.
            r#"b"\0B\0\0\0\0\0\0\0Na\xbc\0\0\0\0\0\0\0\0\0\0\0\0\00'\x0c\x0ctest/message\x03\x02\x04\0\x04\rHello, world!@\x04\0\xbcaN"
"#
        ].assert_debug_eq(&buf);

        let deserialized = RetainedMessages::deserialize(&buf).unwrap();

        let message = deserialized.messages.values().next().unwrap();

        assert_eq!(message.timestamp, 12345678);
        assert_eq!(message.transaction, 0);
        assert_eq!(message.publish, publish);
    }
}
