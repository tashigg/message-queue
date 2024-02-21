use slotmap::SlotMap;
use std::hash::Hash;
use tashi_collections::HashMap;

mod filter;
mod node;
mod visitor;

use filter::Filter;
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

/// A UTF-8 string containing no `\0` characters nor any operators.
struct NameToken<'a>(&'a str);

enum ParseError {
    // Saw a UTF-8 `\0` character.
    FoundNull,
}
