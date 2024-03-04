#[cfg(feature = "arbitrary")]
use arbitrary::Arbitrary;
use slotmap::SlotMap;
use std::fmt::{Debug, Write};
use std::hash::Hash;
mod filter;
mod node;
mod visitor;

pub use filter::{Filter, FilterParseError};

use node::{Data, Node, NodeId};

use crate::mqtt::trie::filter::{FilterToken, LeafKind};
use crate::mqtt::trie::visitor::WalkFilter;

pub struct FilterTrieMultiMap<K, V> {
    root: NodeId,
    nodes: SlotMap<NodeId, Node<K, V>>,
}

impl<K, V> Debug for FilterTrieMultiMap<K, V>
where
    Data<K, V>: Debug,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut debug_map = f.debug_map();

        #[derive(Clone, Copy)]
        enum State {
            Initial { root: NodeId },
            Running { last: NodeId },
        }
        struct Iter<'a, K, V> {
            state: State,
            nodes: &'a SlotMap<NodeId, Node<K, V>>,
        }

        // fixme: This iterator is very inefficient (O(n) scans per tree depth) but it probably doesn't matter because this is a Debug impl.
        // it's O(1) memory usage though.
        impl<'a, K, V> Iterator for Iter<'a, K, V> {
            type Item = NodeId;

            fn next(&mut self) -> Option<Self::Item> {
                fn walk_down<K, V>(
                    mut current: NodeId,
                    nodes: &SlotMap<NodeId, Node<K, V>>,
                ) -> NodeId {
                    while let Some(child) = nodes[current].filters.first() {
                        current = child.1;
                    }

                    current
                }

                let last = match self.state {
                    State::Initial { root } => {
                        let current = walk_down(root, self.nodes);

                        self.state = State::Running { last: current };

                        return Some(current);
                    }
                    State::Running { last } => last,
                };

                let node = &self.nodes[last];
                if node.is_root() {
                    return None;
                }

                let parent = node.parent;
                let node = &self.nodes[parent];
                // find the location of `last` in parent.
                let idx = node.filters.iter().position(|(_, it)| *it == last).unwrap();

                // if parent has any more children, descend into them.
                if let Some(next) = node.filters.get(idx + 1) {
                    let current = walk_down(next.1, self.nodes);

                    self.state = State::Running { last: current };

                    return Some(current);
                }

                // otherwise just return that node (post-order).
                self.state = State::Running { last: parent };

                Some(parent)
            }
        }

        fn walk_up<K, V>(node: NodeId, nodes: &SlotMap<NodeId, Node<K, V>>) -> Vec<FilterToken> {
            let mut current = node;
            let mut tokens = Vec::new();
            while !nodes[current].is_root() {
                let parent = nodes[current].parent;
                let token = &nodes[parent]
                    .filters
                    .iter()
                    .find(|it| it.1 == current)
                    .unwrap()
                    .0;

                tokens.push(token.clone());
                current = parent;
            }

            tokens.reverse();
            tokens
        }

        struct DebugFilter<'a>(&'a [FilterToken], LeafKind);

        impl Debug for DebugFilter<'_> {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_char('"')?;
                let mut needs_sep = false;
                for token in self.0 {
                    if needs_sep {
                        f.write_char('/')?;
                    }
                    match token {
                        FilterToken::Literal(it) => write!(f, "{}", it.escape_debug())?,
                        FilterToken::WildPlus => f.write_char('+')?,
                    }

                    needs_sep = true;
                }

                if matches!(self.1, LeafKind::Any) {
                    if needs_sep {
                        f.write_char('/')?;
                    }
                    f.write_str("#")?;
                }

                f.write_char('"')
            }
        }

        let iter = Iter {
            state: State::Initial { root: self.root },
            nodes: &self.nodes,
        };

        for node_id in iter {
            let tokens = walk_up(node_id, &self.nodes);

            let node = &self.nodes[node_id];
            if let Some(descendant) = &node[LeafKind::Any] {
                debug_map.entry(&DebugFilter(&tokens, LeafKind::Any), descendant);
            }

            if let Some(exact) = &node[LeafKind::Exact] {
                debug_map.entry(&DebugFilter(&tokens, LeafKind::Exact), exact);
            }
        }

        debug_map.finish()
    }
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
                        .filters
                        .insert(place.idx, (place.token, new_id));
                    current = new_id;
                }
            }
        };

        self.nodes[end][leaf_kind]
            .get_or_insert_with(Default::default)
            .insert(key, value)
    }

    pub fn visit_matches(&self, topic_name: &TopicName<'_>, mut f: impl FnMut(&K, &V)) {
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

        let ret = leaf.as_mut()?.remove(key);

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
                .filters
                .iter()
                .position(|it| it.1 == current)
                .expect("orphaned node reached through parent");

            self.nodes[new].filters.remove(idx);

            current = new
        }

        ret
    }
}

#[derive(Debug)]
pub struct TopicName<'a>(Vec<NameToken<'a>>);

#[cfg(feature = "arbitrary")]
impl<'a> Arbitrary<'a> for TopicName<'a> {
    fn arbitrary(u: &mut arbitrary::Unstructured<'a>) -> arbitrary::Result<Self> {
        let s: &'a str = u.arbitrary()?;

        match Self::parse(s) {
            Ok(it) => Ok(it),
            Err(ParseError::UnexpectedCharacter { ch: _, idx }) => {
                Ok(Self::parse(&s[..idx]).unwrap())
            }
        }
    }
}

impl<'a> TopicName<'a> {
    pub fn parse(s: &'a str) -> Result<Self, ParseError> {
        if let Some((idx, ch)) = s.char_indices().find(|it| matches!(it.1, '#' | '+' | '\0')) {
            return Err(ParseError::UnexpectedCharacter { ch, idx });
        }

        let res = s.split('/').map(NameToken).collect();

        Ok(Self(res))
    }
}
impl<'a> TryFrom<&'a str> for TopicName<'a> {
    type Error = ParseError;

    fn try_from(value: &'a str) -> Result<Self, Self::Error> {
        Self::parse(value)
    }
}

/// A UTF-8 string containing no `\0` characters nor any operators.
#[derive(Copy, Clone, Debug)]
struct NameToken<'a>(&'a str);

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

        trie.visit_matches(&TopicName::parse(topic_name).unwrap(), |_k, v| {
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
        "##]]
        .assert_debug_eq(&matches_sorted(&trie, "/nested-0"));

        expect_test::expect![[r##"
            [
                "#",
                "foo/#",
            ]
        "##]]
        .assert_debug_eq(&matches_sorted(&trie, "foo/"));
    }
}
