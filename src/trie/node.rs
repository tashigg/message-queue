use std::ops::{Index, IndexMut};

use slotmap::Key;

use super::filter::{FilterToken, LeafKind};
use super::Leaf;

slotmap::new_key_type! { pub(super) struct NodeId; }

pub(super) struct InternalNode<K, V> {
    pub(super) filters: Vec<(FilterToken, NodeId)>,

    /// Matches if we reach here.
    pub(super) any_leaf: Option<Leaf<K, V>>,
}

// this can't be a derive, derive uses type bounds, but all of these defaults don't need the type bound.
impl<K, V> Default for InternalNode<K, V> {
    fn default() -> Self {
        Self {
            filters: Default::default(),
            any_leaf: Default::default(),
        }
    }
}

pub(super) struct Node<K, V> {
    pub(super) base: InternalNode<K, V>,

    /// Matches if this is the end of the topic name.
    pub(super) leaf: Option<Leaf<K, V>>,

    pub(super) parent: NodeId,
}

impl<K, V> Node<K, V> {
    /// Creates an empty node with the given parent.
    pub(super) fn new(parent: NodeId) -> Self {
        Self {
            base: InternalNode::default(),
            leaf: None,
            parent,
        }
    }

    pub(super) fn root() -> Self {
        Self::new(NodeId::null())
    }

    pub(super) fn is_root(&self) -> bool {
        self.parent.is_null()
    }

    pub(super) fn is_empty(&self) -> bool {
        self.base.filters.is_empty() && self.base.any_leaf.is_none() && self.leaf.is_none()
    }
}

impl<K, V> Index<LeafKind> for Node<K, V> {
    type Output = Option<Leaf<K, V>>;

    fn index(&self, index: LeafKind) -> &Self::Output {
        match index {
            LeafKind::Exact => &self.leaf,
            LeafKind::Any => &self.base.any_leaf,
        }
    }
}

impl<K, V> IndexMut<LeafKind> for Node<K, V> {
    fn index_mut(&mut self, index: LeafKind) -> &mut Self::Output {
        match index {
            LeafKind::Exact => &mut self.leaf,
            LeafKind::Any => &mut self.base.any_leaf,
        }
    }
}
