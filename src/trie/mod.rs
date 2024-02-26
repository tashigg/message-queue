use slotmap::SlotMap;
use std::hash::Hash;
use tashi_collections::HashMap;

mod filter;
mod node;
mod visitor;

pub use filter::{Filter, FilterParseError};

use node::{Node, NodeId};

use crate::trie::visitor::{FilterVisitor, WalkFilter};

pub struct FilterTrieMultiMap<K, V> {
    root: NodeId,
    nodes: SlotMap<NodeId, Node<K, V>>,
}

impl<K, V> FilterTrieMultiMap<K, V> {
    pub fn new() -> Self {
        Self::default()
    }
}

impl<K, V> Default for FilterTrieMultiMap<K, V> {
    fn default() -> Self {
        let mut nodes = SlotMap::default();
        let root = nodes.insert(Node::root());

        Self { nodes, root }
    }
}

impl<K: Eq + Hash, V> FilterTrieMultiMap<K, V> {
    /// Insert a new filter+key into the map
    ///
    /// Returns the old value for this filter, if present.
    pub fn insert(&mut self, filter: Filter, key: K, value: V) -> Option<V> {
        let leaf_kind = filter.leaf_kind;
        let mut visitor = WalkFilter::new(filter);

        let mut current = self.root;
        let end = loop {
            match visitor.visit_node(&self.nodes, current) {
                Ok(res) => break res,
                // expand any missing nodes (we need to insert a leaf at the end after all).
                Err(place) => {
                    let new_id = self.nodes.insert(Node::new(place.parent_id));
                    self.nodes[place.parent_id]
                        .base
                        .filters
                        .insert(place.idx, (place.token, new_id));
                    current = new_id;
                }
            }
        };

        self.nodes[end][leaf_kind]
            .get_or_insert_with(Default::default)
            .0
            .insert(key, value)
    }

    pub fn visit_matches<F: FnMut(&K, &V)>(&self, topic_name: &TopicName<'_>, mut f: F) {
        debug_assert!(!topic_name.0.is_empty(), "invalid empty topic name");
        visitor::VisitMatches::new(&topic_name.0, &mut f).visit_node(&self.nodes, self.root)
    }

    pub fn remove(&mut self, filter: Filter, key: &K) -> Option<V> {
        let leaf_kind = filter.leaf_kind;
        let mut visitor = WalkFilter::new(filter);

        // if we don't have the node for the filter, we certainly won't have the key/value.
        let node = visitor.visit_node(&self.nodes, self.root).ok()?;

        let end = &mut self.nodes[node];

        let leaf = &mut end[leaf_kind];

        let ret = leaf.as_mut()?.0.remove(key);

        // this loop nees to be weirdly split over iterations,
        // iteratively remove nodes from the trie until we find either the root or a non empty node.
        let mut current = node;
        loop {
            let node = &mut self.nodes[current];

            if !node.is_empty() {
                break;
            }

            // don't remove the root node, we'll have to put it back and it's not like it gives us anything useful to get rid of.
            if node.is_root() {
                break;
            }

            let new = node.parent;

            // the expect here failing implies that the node has a different parent than what it thinks,
            let idx = self.nodes[new]
                .base
                .filters
                .iter()
                .position(|it| it.1 == current)
                .expect("orphaned node reached through parent");

            self.nodes[new].base.filters.remove(idx);

            current = new
        }

        ret
    }
}

struct Leaf<K, V>(HashMap<K, V>);

impl<K, V> Default for Leaf<K, V> {
    fn default() -> Self {
        Self(Default::default())
    }
}

pub struct TopicName<'a>(Vec<NameToken<'a>>);

impl<'a> TopicName<'a> {
    pub fn from_str(s: &'a str) -> Result<Self, ParseError> {
        if let Some((idx, ch)) = s.char_indices().find(|it| matches!(it.1, '#' | '+' | '\0')) {
            return Err(ParseError::UnexpectedCharacter { ch, idx });
        }

        let res = s.split('/').map(NameToken).collect();

        Ok(Self(res))
    }
}

/// A UTF-8 string containing no `\0` characters nor any operators.
#[derive(Copy, Clone)]
pub struct NameToken<'a>(&'a str);

#[derive(thiserror::Error, Debug)]
pub enum ParseError {
    /// Found an unexpected character `ch` at `idx`.
    #[error("unexpected character `{ch}` at {idx}`")]
    UnexpectedCharacter { ch: char, idx: usize },
}

#[cfg(test)]
mod tests {
    use super::filter::Filter;
    use super::{FilterTrieMultiMap, TopicName};

    #[test]
    fn insert_remove() {
        #[derive(Eq, PartialEq)]
        struct NoClone<T>(T);
        let filter: Filter = "foo/bar".parse().unwrap();
        let mut trie = FilterTrieMultiMap::new();
        trie.insert(filter.clone(), 0, NoClone(0));

        assert_eq!(trie.remove(filter.clone(), &0).map(|it| it.0), Some(0));

        let values = [
            ("foo", 0),
            ("foo/bar", 0),
            ("foo/baz", 0),
            ("foo/#", 0),
            ("foo/baz", 1),
            ("#", 2),
        ];

        for (idx, &(filter, key)) in values.clone().iter().enumerate() {
            trie.insert(filter.parse().unwrap(), key, NoClone(idx as i32 + 1));
        }

        for (idx, &(filter, key)) in values.iter().enumerate() {
            assert_eq!(
                trie.remove(filter.parse().unwrap(), &key).map(|it| it.0),
                Some(idx as i32 + 1)
            );
        }
    }

    #[track_caller]
    fn matches_sorted<V: Ord + Copy>(
        trie: &FilterTrieMultiMap<i32, V>,
        topic_name: &str,
    ) -> Vec<V> {
        let mut seen = Vec::new();

        trie.visit_matches(&TopicName::from_str(topic_name).unwrap(), |_k, v| {
            seen.push(*v);
        });

        seen.sort();

        seen
    }

    #[test]
    fn all_matches() {
        let values = [
            ("foo", 0),
            ("foo/bar", 0),
            ("foo/baz", 0),
            ("+/bar", 1),
            ("foo/#", 0),
            ("foo/baz", 1),
            ("#", 2),
            ("foo//", 0),
            ("/+", 0),
        ];

        let mut trie = FilterTrieMultiMap::new();

        for &(filter, key) in values.clone().iter() {
            trie.insert(filter.parse().unwrap(), key, filter);
        }

        expect_test::expect![[r##"
            [
                "#",
                "+/bar",
                "foo/#",
                "foo/bar",
            ]
        "##]]
        .assert_debug_eq(&matches_sorted(&trie, "foo/bar"));

        expect_test::expect![[r##"
            [
                "#",
                "foo/#",
                "foo/baz",
                "foo/baz",
            ]
        "##]]
        .assert_debug_eq(&matches_sorted(&trie, "foo/baz"));

        expect_test::expect![[r##"
            [
                "#",
                "foo",
            ]
        "##]]
        .assert_debug_eq(&matches_sorted(&trie, "foo"));

        expect_test::expect![[r##"
            [
                "#",
                "+/bar",
            ]
        "##]]
        .assert_debug_eq(&matches_sorted(&trie, "bar$/bar"));

        expect_test::expect![[r##"
            [
                "#",
            ]
        "##]]
        .assert_debug_eq(&matches_sorted(&trie, "bar$"));

        expect_test::expect![[r##"
            [
                "#",
                "/+",
            ]
        "##]].assert_debug_eq(&matches_sorted(&trie, "/nested-0"));

        expect_test::expect![[r##"
            [
                "#",
                "foo/#",
            ]
        "##]]
        .assert_debug_eq(&matches_sorted(&trie, "foo/"));
    }
}
