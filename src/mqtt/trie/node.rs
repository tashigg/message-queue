use std::ops::{Index, IndexMut};

use slotmap::Key;
use tashi_collections::HashMap;

use super::filter::{FilterToken, LeafKind};

/// A leaf of a trie, currently a `HashMap<K, V>`, but could end up as a `T``
pub(super) type Data<K, V> = HashMap<K, V>;

slotmap::new_key_type! { pub(super) struct NodeId; }

/// Leaf values for a node.
///
/// This can be considered it's own node type, but every internal node has exactly one leaf, so they simply own the leaves.
pub(super) struct NodeLeaf<K, V> {
    /// Corresponds to [`LeafKind::Exact`], matches the end of a topic name.
    pub(super) exact_val: Option<Data<K, V>>,
    // fixme: what's a better word than `non-end`?
    /// Corresponds to [`LeafKind::Any`], matches the non-end of a topic name.
    pub(super) descendant_val: Option<Data<K, V>>,
}

impl<K, V> NodeLeaf<K, V> {
    fn is_empty(&self) -> bool {
        // this could be a matches!, but, no, it'd be a very hard to understand `matches!`.
        #[allow(clippy::match_like_matches_macro)]
        match (&self.exact_val, &self.descendant_val) {
            (Some(it), _) | (_, Some(it)) if !it.is_empty() => false,
            _ => true,
        }
    }
}

impl<K, V> Default for NodeLeaf<K, V> {
    fn default() -> Self {
        Self {
            exact_val: None,
            descendant_val: None,
        }
    }
}

/// An internal node of the trie.
pub(super) struct Node<K, V> {
    pub(super) parent: NodeId,

    // All internal nodes have exactly one leaf node,
    // so it's simpler to just pretend like they aren't really nodes.
    // since all internal nodes have exactly one leaf,
    // and all leaves correspond to exactly one node, the node owns the leaf.
    pub(super) leaf_data: NodeLeaf<K, V>,

    pub(super) filters: Vec<(FilterToken, NodeId)>,
}

impl<K, V> Node<K, V> {
    /// Creates an empty node with the given parent.
    pub(super) fn new(parent: NodeId) -> Self {
        Self {
            parent,
            leaf_data: Default::default(),
            filters: Vec::default(),
        }
    }

    pub(super) fn root() -> Self {
        Self::new(NodeId::null())
    }

    pub(super) fn is_root(&self) -> bool {
        self.parent.is_null()
    }

    pub(super) fn is_empty(&self) -> bool {
        self.filters.is_empty() && self.leaf_data.is_empty()
    }
}

impl<K, V> Index<LeafKind> for Node<K, V> {
    type Output = Option<Data<K, V>>;

    fn index(&self, index: LeafKind) -> &Self::Output {
        match index {
            LeafKind::Exact => &self.leaf_data.exact_val,
            LeafKind::Any => &self.leaf_data.descendant_val,
        }
    }
}

impl<K, V> IndexMut<LeafKind> for Node<K, V> {
    fn index_mut(&mut self, index: LeafKind) -> &mut Self::Output {
        match index {
            LeafKind::Exact => &mut self.leaf_data.exact_val,
            LeafKind::Any => &mut self.leaf_data.descendant_val,
        }
    }
}
