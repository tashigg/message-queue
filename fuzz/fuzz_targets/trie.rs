#![no_main]

use std::hint::black_box;

use libfuzzer_sys::arbitrary::Arbitrary;
use libfuzzer_sys::fuzz_target;
use tashi_message_queue::mqtt::trie::{Filter, FilterTrie, TopicName};

#[derive(Debug, Arbitrary)]
struct Data<'a> {
    filters: Vec<Filter>,
    topic_names: Vec<TopicName<'a>>,
    commands: Vec<Command>,
}

#[derive(Debug, Arbitrary)]
enum Command {
    Insert { filter: usize },
    InsertExact { topic_name: usize },
    Remove { filter: usize },
    Find { topic_name: usize },
    Debug,
}

fuzz_target!(|data: Data<'_>| {
    let mut trie: FilterTrie<()> = FilterTrie::new();

    for command in data.commands {
        match command {
            Command::Insert { filter } => {
                if let Some(filter) = data.filters.get(filter).cloned() {
                    let _ = black_box(trie.insert(filter, ()));
                }
            }
            Command::InsertExact { topic_name } => {
                if let Some(topic_name) = data.topic_names.get(topic_name) {
                    let _ = black_box(trie.insert(Filter::from(topic_name), ()));
                }
            }
            Command::Remove { filter } => {
                if let Some(filter) = data.filters.get(filter) {
                    let _ = black_box(trie.remove_by_filter(filter));
                }
            }
            Command::Find { topic_name } => {
                if let Some(topic_name) = data.topic_names.get(topic_name) {
                    trie.visit_matches(topic_name, |it| {
                        black_box(it);
                    });
                }
            }
            Command::Debug => {
                let _ = black_box(format!("{:?}", trie));
            }
        }
    }
});
