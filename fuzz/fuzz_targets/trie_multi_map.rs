#![no_main]

use std::hint::black_box;

use libfuzzer_sys::arbitrary::Arbitrary;
use libfuzzer_sys::fuzz_target;
use tashi_message_queue::mqtt::trie::{Filter, FilterTrieMultiMap, TopicName};

#[derive(Debug, Arbitrary)]
struct Data<'a> {
    filters: Vec<Filter>,
    topic_names: Vec<TopicName<'a>>,
    commands: Vec<Command>,
}

#[derive(Debug, Arbitrary)]
enum Command {
    Insert { key: u16, filter: usize },
    InsertExact { key: u16, topic_name: usize },
    Remove { key: u16, filter: usize },
    Find { topic_name: usize },
    Debug,
}

fuzz_target!(|data: Data<'_>| {
    let mut trie: FilterTrieMultiMap<u16, ()> =
        tashi_message_queue::mqtt::trie::FilterTrieMultiMap::new();

    for command in data.commands {
        match command {
            Command::Insert { key, filter } => {
                if let Some(filter) = data.filters.get(filter).cloned() {
                    let _ = black_box(trie.insert(filter, key, ()));
                }
            }
            Command::InsertExact { key, topic_name } => {
                if let Some(topic_name) = data.topic_names.get(topic_name) {
                    let _ = black_box(trie.insert(Filter::from(topic_name), key, ()));
                }
            }
            Command::Remove { key, filter } => {
                if let Some(filter) = data.filters.get(filter).cloned() {
                    let _ = black_box(trie.remove_by_filter(&filter, &key));
                }
            }
            Command::Find { topic_name } => {
                if let Some(topic_name) = data.topic_names.get(topic_name) {
                    trie.visit_matches(topic_name, |k, v| {
                        black_box((k, v));
                    });
                }
            }
            Command::Debug => {
                let _ = black_box(format!("{:?}", trie));
            }
        }
    }
});
