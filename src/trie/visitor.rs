use std::vec;

use slotmap::SlotMap;

use super::filter::FilterToken;
use super::node::{Node, NodeId};
use super::Leaf;

pub(super) type VisitContext<K, V> = SlotMap<NodeId, Node<K, V>>;

pub(super) trait FilterVisitor<T, K, V> {
    fn visit_node(&mut self, cx: &VisitContext<K, V>, node_id: NodeId) -> T;
    fn visit_leaf(&mut self, leaf: &Leaf<K, V>, node_id: NodeId) -> T;
}

/// A resumable visitor to walk down a filter path.
///
/// Note: Resumable in this context means that if the visitor returns an error,
/// inserting a node at given [`NodePlace`] then calling [`visit_node`] again with the newly inserted node continues the walk where it left off.
///
/// [`visit_node`]: FilterVisitor::visit_node
pub(super) struct WalkFilter(vec::IntoIter<FilterToken>);

impl WalkFilter {
    pub(super) fn new(filter: super::Filter) -> Self {
        assert!(
            filter.leaf_kind.is_any() || !filter.tokens.is_empty(),
            "BUG: empty filter"
        );
        Self(filter.tokens.into_iter())
    }
}

impl<K, V> FilterVisitor<Result<NodeId, NodePlace>, K, V> for WalkFilter {
    fn visit_node(
        &mut self,
        cx: &VisitContext<K, V>,
        node_id: NodeId,
    ) -> Result<NodeId, NodePlace> {
        let mut node_id = node_id;

        for token in self.0.by_ref() {
            let node = &cx[node_id];

            let idx = node
                .base
                .filters
                .binary_search_by_key(&&token, |it| &it.0)
                .map_err(|idx| NodePlace {
                    parent_id: node_id,
                    token,
                    idx,
                })?;

            node_id = node.base.filters[idx].1;
        }

        // we ran out of filters this is the node.
        Ok(node_id)
    }

    fn visit_leaf(&mut self, _leaf: &Leaf<K, V>, _node_id: NodeId) -> Result<NodeId, NodePlace> {
        unimplemented!()
    }
}

/// An identification on where to insert a node if it's missing.
pub(super) struct NodePlace {
    pub(super) parent_id: NodeId,
    pub(super) token: FilterToken,
    pub(super) idx: usize,
}
