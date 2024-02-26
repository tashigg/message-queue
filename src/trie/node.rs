use std::ops::{Index, IndexMut};

use slotmap::Key;
use tashi_collections::HashMap;

use super::filter::{FilterToken, LeafKind};

/// A leaf of a trie, currently a `HashMap<K, V>`, but could end up as a `T``
pub(super) type Leaf<K, V> = HashMap<K, V>;

slotmap::new_key_type! { pub(super) struct NodeId; }

pub(super) struct Node<K, V> {
    pub(super) parent: NodeId,

    pub(super) filters: Vec<(FilterToken, NodeId)>,

    /// Matches if this is the end of the topic name.
    pub(super) leaf: Option<Leaf<K, V>>,

    /// Matches a topic when this is *not* the end of the topic name.
    pub(super) descendant_leaf: Option<Leaf<K, V>>,
}

impl<K, V> Node<K, V> {
    /// Creates an empty node with the given parent.
    pub(super) fn new(parent: NodeId) -> Self {
        Self {
            parent,
            leaf: None,
            descendant_leaf: None,
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
        self.filters.is_empty() && self.descendant_leaf.is_none() && self.leaf.is_none()
    }
}

impl<K, V> Index<LeafKind> for Node<K, V> {
    type Output = Option<Leaf<K, V>>;

    fn index(&self, index: LeafKind) -> &Self::Output {
        match index {
            LeafKind::Exact => &self.leaf,
            LeafKind::Any => &self.descendant_leaf,
        }
    }
}

impl<K, V> IndexMut<LeafKind> for Node<K, V> {
    fn index_mut(&mut self, index: LeafKind) -> &mut Self::Output {
        match index {
            LeafKind::Exact => &mut self.leaf,
            LeafKind::Any => &mut self.descendant_leaf,
        }
    }
}
