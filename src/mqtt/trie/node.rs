use std::ops::{Index, IndexMut};

use slotmap::Key;

use super::filter::{FilterToken, LeafKind};

slotmap::new_key_type! { pub(super) struct NodeId; }

/// Leaf values for a node.
///
/// This can be considered it's own node type, but every internal node has exactly one leaf, so they simply own the leaves.
pub(super) struct NodeLeaf<T> {
    /// Corresponds to [`LeafKind::Exact`], matches the end of a topic name.
    pub(super) exact_val: Option<T>,
    // fixme: what's a better word than `non-end`?
    /// Corresponds to [`LeafKind::Any`], matches the non-end of a topic name.
    pub(super) descendant_val: Option<T>,
}

impl<T> NodeLeaf<T> {
    pub(super) fn all(&self, f: &mut impl FnMut(&T) -> bool) -> bool {
        self.exact_val.as_ref().is_none_or(&mut *f) && self.descendant_val.as_ref().is_none_or(f)
    }

    fn is_empty(&self) -> bool {
        self.exact_val.is_none() && self.descendant_val.is_none()
    }
}

impl<T> Default for NodeLeaf<T> {
    fn default() -> Self {
        Self {
            exact_val: None,
            descendant_val: None,
        }
    }
}

/// An internal node of the trie.
pub(super) struct Node<T> {
    pub(super) parent: NodeId,

    // All internal nodes have exactly one leaf node,
    // so it's simpler to just pretend like they aren't really nodes.
    // since all internal nodes have exactly one leaf,
    // and all leaves correspond to exactly one node, the node owns the leaf.
    pub(super) leaf_data: NodeLeaf<T>,

    pub(super) filters: Vec<(FilterToken, NodeId)>,
}

impl<T> Node<T> {
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

    pub(super) fn is_empty(&self) -> bool {
        self.filters.is_empty() && self.leaf_data.is_empty()
    }

    pub(super) fn is_root(&self) -> bool {
        self.parent.is_null()
    }
}

impl<T> Index<LeafKind> for Node<T> {
    type Output = Option<T>;

    fn index(&self, index: LeafKind) -> &Self::Output {
        match index {
            LeafKind::Exact => &self.leaf_data.exact_val,
            LeafKind::Any => &self.leaf_data.descendant_val,
        }
    }
}

impl<T> IndexMut<LeafKind> for Node<T> {
    fn index_mut(&mut self, index: LeafKind) -> &mut Self::Output {
        match index {
            LeafKind::Exact => &mut self.leaf_data.exact_val,
            LeafKind::Any => &mut self.leaf_data.descendant_val,
        }
    }
}
