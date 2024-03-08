use super::filter::{FilterToken, LeafKind};
use super::node::NodeId;
use super::NameToken;

/// An identification on where to insert a node if it's missing.
pub(super) struct NodePlace {
    pub(super) parent_id: NodeId,
    pub(super) token: usize,
    pub(super) idx: usize,
}

pub(super) fn walk_filter<T>(
    filter: &[FilterToken],
    cx: &super::Nodes<T>,
    root: NodeId,
) -> Result<NodeId, NodePlace> {
    let mut node_id = root;
    for (token_idx, token) in filter.iter().enumerate() {
        let node = &cx[node_id];

        let idx = node
            .filters
            .binary_search_by_key(&token, |it| &it.0)
            .map_err(|idx| NodePlace {
                parent_id: node_id,
                token: token_idx,
                idx,
            })?;

        node_id = node.filters[idx].1;
    }

    // we ran out of filters this is the node.
    Ok(node_id)
}

pub(super) fn visit_matches<T, F: FnMut(&T)>(
    topic_name: &[NameToken<'_>],
    cx: &super::Nodes<T>,
    node_id: NodeId,
    callback: &mut F,
) {
    let node = &cx[node_id];

    let Some((&next, rest)) = topic_name.split_first() else {
        if let Some(leaf) = &node[LeafKind::Exact] {
            callback(leaf)
        }

        return;
    };

    // important note: `descendant_leaf` is tacked onto this node because there's no node that makes sense to do so otherwise.
    // However, it doesn't match when an exact match would. `foo/#` (Filter) doesn't match `foo` (Topic name), but `foo` (Filter) would.
    if let Some(leaf) = &node[LeafKind::Any] {
        callback(leaf)
    }

    for (_, node_id) in node.filters.iter().filter(|it| it.0.matches(next)) {
        visit_matches(rest, cx, *node_id, &mut *callback);
    }
}
