use std::vec;

use slotmap::SlotMap;

use super::filter::FilterToken;
use super::node::{Node, NodeId};
use super::{Leaf, NameToken};

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
                .filters
                .binary_search_by_key(&&token, |it| &it.0)
                .map_err(|idx| NodePlace {
                    parent_id: node_id,
                    token,
                    idx,
                })?;

            node_id = node.filters[idx].1;
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

/// Visits all filters that match the topic in an unspecified order.
/// All matching topics will be visited exactly once.
pub(super) struct VisitMatches<'a, 'b: 'a, F> {
    topic_name: &'a [NameToken<'b>],
    callback: &'a mut F,
}

impl<'a, 'b: 'a, F: 'a> VisitMatches<'a, 'b, F> {
    pub(super) fn new(topic_name: &'a [NameToken<'b>], callback: &'a mut F) -> Self {
        Self {
            topic_name,
            callback,
        }
    }
}

impl<'a, 'b: 'a, K, V, F: FnMut(&K, &V)> FilterVisitor<(), K, V> for VisitMatches<'a, 'b, F> {
    fn visit_node(&mut self, cx: &VisitContext<K, V>, node_id: NodeId) {
        let node = &cx[node_id];

        let Some((&next, rest)) = self.topic_name.split_first() else {
            if let Some(leaf) = &node.leaf {
                self.visit_leaf(leaf, node_id)
            }

            return;
        };

        // important note: `descendant_leaf` is tacked onto this node because there's no node that makes sense to do so otherwise.
        // However, it doesn't match when an exact match would. `foo/#` (Filter) doesn't match `foo` (Topic name), but `foo` (Filter) would.
        if let Some(leaf) = &node.descendant_leaf {
            self.visit_leaf(leaf, node_id)
        }

        let mut visitor = VisitMatches::new(rest, &mut *self.callback);

        for (_, node_id) in node.filters.iter().filter(|it| it.0.matches(next)) {
            visitor.visit_node(cx, *node_id);
        }
    }

    fn visit_leaf(&mut self, leaf: &Leaf<K, V>, _node_id: NodeId) {
        for (k, v) in leaf.iter() {
            (self.callback)(k, v)
        }
    }
}
