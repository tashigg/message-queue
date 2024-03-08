#[cfg(feature = "arbitrary")]
use arbitrary::Arbitrary;
use slotmap::SlotMap;
use std::fmt::{Debug, Display, Formatter, Write};
use std::hash::Hash;

mod filter;
mod node;
mod visitor;

pub use filter::Filter;

use node::{Data, Node, NodeId};

use filter::{FilterToken, LeafKind};
use visitor::NodePlace;

type Nodes<T> = SlotMap<NodeId, Node<T>>;

pub struct FilterTrie<T> {
    root: NodeId,
    nodes: Nodes<T>,
}

/// An opaque index into a [`FilterTrieMultiMap`] for quickly finding an entry
/// without traversing the whole tree.
#[derive(Copy, Clone, Hash, PartialEq, Eq, Debug)]
pub struct EntryId {
    node_id: NodeId,
    leaf_kind: LeafKind,
}

impl<T> Debug for FilterTrie<T>
where
    T: Debug,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut debug_map = f.debug_map();

        #[derive(Clone, Copy)]
        enum State {
            Initial { root: NodeId },
            Running { last: NodeId },
        }
        struct Iter<'a, T> {
            state: State,
            nodes: &'a Nodes<T>,
        }

        // fixme: This iterator is very inefficient (O(n) scans per tree depth) but it probably doesn't matter because this is a Debug impl.
        // it's O(1) memory usage though.
        impl<'a, T> Iterator for Iter<'a, T> {
            type Item = NodeId;

            fn next(&mut self) -> Option<Self::Item> {
                fn walk_down<T>(mut current: NodeId, nodes: &Nodes<T>) -> NodeId {
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

        fn walk_up<T>(node: NodeId, nodes: &Nodes<T>) -> Vec<FilterToken> {
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

impl<T> FilterTrie<T> {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn contains_entry(&self, entry_id: EntryId) -> bool {
        self.nodes
            .get(entry_id.node_id)
            .map_or(false, |it| it[entry_id.leaf_kind].is_some())
    }

    fn lookup(&self, filter: &Filter) -> Option<EntryId> {
        let node_id = visitor::walk_filter(&filter.tokens, &self.nodes, self.root).ok()?;

        Some(EntryId {
            node_id,
            leaf_kind: filter.leaf_kind,
        })
    }

    pub fn insert(&mut self, filter: Filter, value: T) -> Option<T> {
        match self.entry(filter) {
            Entry::Occupied(mut entry) => Some(entry.insert(value)),
            Entry::Vacant(entry) => {
                entry.insert(value);
                None
            }
        }
    }

    pub fn get(&self, filter: &Filter) -> Option<&T> {
        let entry = self.lookup(filter)?;
        self.nodes[entry.node_id][entry.leaf_kind].as_ref()
    }

    pub fn get_mut(&mut self, filter: &Filter) -> Option<&mut T> {
        let entry = self.lookup(filter)?;
        self.nodes[entry.node_id][entry.leaf_kind].as_mut()
    }

    pub fn entry(&mut self, filter: Filter) -> Entry<'_, T> {
        match visitor::walk_filter(&filter.tokens, &self.nodes, self.root) {
            Ok(end) => {
                let entry_id = EntryId {
                    node_id: end,
                    leaf_kind: filter.leaf_kind,
                };

                match self.contains_entry(entry_id) {
                    true => Entry::Occupied(OccupiedEntry {
                        filter,
                        base: IdEntry {
                            entry_id,
                            nodes: &mut self.nodes,
                        },
                    }),
                    false => Entry::Vacant(VacantEntry {
                        filter,
                        place: PlaceOrLeaf::Leaf(end),
                        nodes: &mut self.nodes,
                    }),
                }
            }
            Err(place) => Entry::Vacant(VacantEntry {
                filter,
                place: PlaceOrLeaf::Place(place),
                nodes: &mut self.nodes,
            }),
        }
    }

    pub fn entry_by_id(&mut self, entry_id: EntryId) -> Option<IdEntry<'_, T>> {
        if self.contains_entry(entry_id) {
            return Some(IdEntry {
                entry_id,
                nodes: &mut self.nodes,
            });
        }

        None
    }

    pub fn visit_matches(&self, topic_name: &TopicName<'_>, mut f: impl FnMut(&T)) {
        debug_assert!(!topic_name.0.is_empty(), "invalid empty topic name");
        visitor::VisitMatches::new(&topic_name.0, &mut f).visit_node(&self.nodes, self.root)
    }

    pub fn remove_by_filter(&mut self, filter: &Filter) -> Option<T> {
        let entry_id = self.lookup(filter)?;

        self.entry_by_id(entry_id).map(IdEntry::remove)
    }
}

impl<T> Default for FilterTrie<T> {
    fn default() -> Self {
        let mut nodes = Nodes::default();
        let root = nodes.insert(Node::root());

        Self { nodes, root }
    }
}

pub struct IdEntry<'a, T: 'a> {
    entry_id: EntryId,
    nodes: &'a mut Nodes<T>,
}

impl<'a, T: 'a> IdEntry<'a, T> {
    pub fn entry_id(&self) -> EntryId {
        self.entry_id
    }

    pub fn get(&self) -> &T {
        // fixme: should be a `get_unchecked`.
        let node = &self.nodes[self.entry_id.node_id];

        // should be an `unwrap_unchecked`
        node[self.entry_id.leaf_kind].as_ref().unwrap()
    }

    pub fn get_mut(&mut self) -> &mut T {
        // fixme: should be a `get_unchecked`.
        let node = &mut self.nodes[self.entry_id.node_id];

        // should be an `unwrap_unchecked`
        node[self.entry_id.leaf_kind].as_mut().unwrap()
    }

    pub fn into_mut(self) -> &'a mut T {
        let node = &mut self.nodes[self.entry_id.node_id];

        match &mut node[self.entry_id.leaf_kind] {
            Some(it) => it,
            // should be unchecked
            None => unreachable!(),
        }
    }

    pub fn into_mut_with_id(self) -> (&'a mut T, EntryId) {
        let id = self.entry_id();

        (self.into_mut(), id)
    }

    pub fn insert(&mut self, value: T) -> T {
        // fixme: should be a `get_unchecked`.
        let node = &mut self.nodes[self.entry_id.node_id];

        // should be an `unwrap_unchecked`
        node[self.entry_id.leaf_kind].replace(value).unwrap()
    }

    pub fn remove(self) -> T {
        remove_entry(self.entry_id(), self.nodes, |node| node.is_empty())
    }
}

pub enum Entry<'a, T: 'a> {
    Occupied(OccupiedEntry<'a, T>),
    Vacant(VacantEntry<'a, T>),
}

impl<'a, T: 'a> Entry<'a, T> {
    pub fn or_insert(self, default: T) -> (&'a mut T, EntryId) {
        match self {
            Entry::Occupied(entry) => entry.into_mut_with_id(),
            Entry::Vacant(entry) => entry.insert(default),
        }
    }

    pub fn or_insert_with<F: FnOnce() -> T>(self, default: F) -> (&'a mut T, EntryId) {
        match self {
            Entry::Occupied(entry) => entry.into_mut_with_id(),
            Entry::Vacant(entry) => entry.insert(default()),
        }
    }

    pub fn filter(&self) -> &Filter {
        match self {
            Entry::Occupied(entry) => entry.filter(),
            Entry::Vacant(entry) => entry.filter(),
        }
    }

    pub fn and_modify<F: FnOnce(&mut T)>(self, f: F) -> Self {
        match self {
            Entry::Occupied(mut it) => {
                f(it.get_mut());
                Entry::Occupied(it)
            }
            Entry::Vacant(it) => Entry::Vacant(it),
        }
    }

    pub fn or_default(self) -> (&'a mut T, EntryId)
    where
        T: Default,
    {
        match self {
            Entry::Occupied(entry) => entry.into_mut_with_id(),
            Entry::Vacant(entry) => entry.insert(Default::default()),
        }
    }
}

pub struct OccupiedEntry<'a, T: 'a> {
    // kinda sucks that we have to keep this around just for things to be able to get it back out.
    filter: Filter,
    base: IdEntry<'a, T>,
}

impl<'a, T> OccupiedEntry<'a, T> {
    pub fn filter(&self) -> &Filter {
        &self.filter
    }

    pub fn into_filter(self) -> Filter {
        self.filter
    }

    pub fn entry_id(&self) -> EntryId {
        self.base.entry_id()
    }

    pub fn get(&self) -> &T {
        self.base.get()
    }

    pub fn get_mut(&mut self) -> &mut T {
        self.base.get_mut()
    }

    pub fn into_mut(self) -> &'a mut T {
        self.base.into_mut()
    }

    pub fn into_mut_with_id(self) -> (&'a mut T, EntryId) {
        self.base.into_mut_with_id()
    }

    pub fn insert(&mut self, value: T) -> T {
        self.base.insert(value)
    }

    fn remove_kv(self, empty_node: impl FnMut(&Node<T>) -> bool) -> (Filter, T) {
        let value = remove_entry(self.entry_id(), self.base.nodes, empty_node);
        (self.filter, value)
    }

    pub fn remove_entry(self) -> (Filter, T) {
        self.remove_kv(|node| node.is_empty())
    }

    pub fn remove(self) -> T {
        self.remove_entry().1
    }
}

/// Invariant: `nodes.contains(place_id)` must return `true`,
/// this is not currently a safety invariant.
/// When it becomes one this function will become `unsafe` as would be required.
fn remove_entry<T>(
    place_id: EntryId,
    nodes: &mut Nodes<T>,
    mut empty_node: impl FnMut(&Node<T>) -> bool,
) -> T {
    let EntryId { node_id, leaf_kind } = place_id;
    // fixme: should be a `get_unchecked`.
    let node = &mut nodes[node_id];

    // should be an `unwrap_unchecked`
    let value = node[leaf_kind].take().unwrap();

    // this loop nees to be weirdly split over iterations,
    // iteratively remove nodes from the trie until we find either the root or a non empty node.
    let mut current = node_id;
    loop {
        let node = &mut nodes[current];

        if !empty_node(node) {
            break;
        }

        // don't remove the root node, we'll have to put it back and it's not like it gives us anything useful to get rid of.
        if node.is_root() {
            break;
        }

        let new = node.parent;

        // the expect here failing implies that the node has a different parent than what it thinks,
        let idx = nodes[new]
            .filters
            .iter()
            .position(|it| it.1 == current)
            .expect("orphaned node reached through parent");

        nodes[new].filters.remove(idx);

        current = new
    }

    value
}

// used in a vacant entry to insert nodes.
enum PlaceOrLeaf {
    // there's a missing node.
    Place(NodePlace),
    Leaf(NodeId),
}

pub struct VacantEntry<'a, T> {
    filter: Filter,
    place: PlaceOrLeaf,
    nodes: &'a mut Nodes<T>,
}

impl<'a, T> VacantEntry<'a, T> {
    pub fn filter(&self) -> &Filter {
        &self.filter
    }

    pub fn into_filter(self) -> Filter {
        self.filter
    }

    pub fn insert(self, value: T) -> (&'a mut T, EntryId) {
        let node_id = match self.place {
            PlaceOrLeaf::Place(place) => {
                let mut insertions = self.filter.tokens.into_iter().skip(place.token);

                let first = insertions.next().unwrap();
                let new_id = self.nodes.insert(Node::new(place.parent_id));
                self.nodes[place.parent_id]
                    .filters
                    .insert(place.idx, (first, new_id));
                let mut current = new_id;

                // since we just made a node we know it's empty and don't have to figure out where to push the new nodes.
                for insertion in insertions {
                    let new_id = self.nodes.insert(Node::new(current));
                    self.nodes[current].filters.push((insertion, new_id));
                    current = new_id;
                }

                current
            }
            PlaceOrLeaf::Leaf(node) => node,
        };

        let place_id = EntryId {
            node_id,
            leaf_kind: self.filter.leaf_kind,
        };

        let value = self.nodes[node_id][self.filter.leaf_kind].insert(value);

        (value, place_id)
    }
}

// "specialization" of a `FilterTrie` with some added niceties.
// this is a different struct rather than a `type` because of coherence (blanket impl for T overlaps exact impl for `node::Data<K, V>`).
pub struct FilterTrieMultiMap<K, V> {
    map: FilterTrie<node::Data<K, V>>,
    len: usize,
}

impl<K, V> Default for FilterTrieMultiMap<K, V> {
    fn default() -> Self {
        Self {
            map: Default::default(),
            len: 0,
        }
    }
}

impl<K, V> Debug for FilterTrieMultiMap<K, V>
where
    node::Data<K, V>: Debug,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.map.fmt(f)
    }
}

#[cfg_attr(not(test), allow(dead_code))]
impl<K, V> FilterTrieMultiMap<K, V> {
    pub fn new() -> Self {
        Self::default()
    }
}

#[cfg_attr(not(test), allow(dead_code))]
impl<K: Eq + Hash, V> FilterTrieMultiMap<K, V> {
    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Insert a new filter+key into the map
    ///
    /// Returns the old value for this filter, if present,
    /// as well as an opaque ID for efficiently finding the entry again without traversing the trie.
    pub fn insert(&mut self, filter: Filter, key: K, value: V) -> (EntryId, Option<V>) {
        let (map, place) = self.map.entry(filter).or_default();

        let replaced = map.insert(key, value);
        self.len += replaced.is_none() as usize;

        (place, replaced)
    }

    pub fn visit_matches(&self, topic_name: &TopicName<'_>, mut f: impl FnMut(&K, &V)) {
        debug_assert!(!topic_name.0.is_empty(), "invalid empty topic name");
        visitor::VisitMatches::new(&topic_name.0, &mut |map: &Data<K, V>| {
            map.iter().for_each(|(k, v)| f(k, v))
        })
        .visit_node(&self.map.nodes, self.map.root)
    }

    /// Find and remove a key that was previously added for `filter`.
    pub fn remove_by_filter(&mut self, filter: &Filter, key: &K) -> Option<V> {
        let entry_id = self.map.lookup(filter)?;

        self.remove_by_id(entry_id, key)
    }

    /// Remove a key using a previously returned `EntryId`.
    pub fn remove_by_id(&mut self, entry_id: EntryId, key: &K) -> Option<V> {
        let EntryId { node_id, leaf_kind } = entry_id;

        let end = &mut self.map.nodes[node_id];

        let leaf = &mut end[leaf_kind];

        let v = leaf.as_mut()?.remove(key);

        self.len -= v.is_some() as usize;

        if end.is_multimap_empty() {
            remove_entry(entry_id, &mut self.map.nodes, |node| {
                node.is_multimap_empty()
            });
        }

        v
    }
}

/// A valid Topic Name per the MQTT v5 spec.
///
/// https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901107
///
/// A Topic Name must not be zero-length, nor may it contain wildcard characters or a null byte (`\0`).
///
/// A Topic Name in a `PUBLISH` packet _may_ be empty if a Topic Alias is used,
/// but that alias *must* be resolved to a topic string before calling `parse()`.
#[derive(Debug)]
pub struct TopicName<'a>(Vec<NameToken<'a>>);

#[cfg(feature = "arbitrary")]
impl<'a> Arbitrary<'a> for TopicName<'a> {
    fn arbitrary(u: &mut arbitrary::Unstructured<'a>) -> arbitrary::Result<Self> {
        let s: &'a str = u.arbitrary()?;

        match Self::parse(s) {
            Ok(it) => Ok(it),
            Err(ParseError::TopicEmpty) => Err(arbitrary::Error::NotEnoughData),
            Err(ParseError::UnexpectedCharacter { ch: _, idx }) => {
                if idx == 0 {
                    return Err(arbitrary::Error::NotEnoughData);
                }

                Ok(Self::parse(&s[..idx]).unwrap())
            }
        }
    }
}

impl<'a> TopicName<'a> {
    /// Parse a valid Topic Name as per the MQTT v5 spec. See the type docs for details.
    pub fn parse(s: &'a str) -> Result<Self, ParseError> {
        if s.is_empty() {
            return Err(ParseError::TopicEmpty);
        }

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

impl Display for TopicName<'_> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.root())?;

        for token in &self.0[1..] {
            f.write_char('/')?;
            f.write_str(token.0)?;
        }

        Ok(())
    }
}

impl<'a> TopicName<'a> {
    /// Get the root of the topic, i.e. the first segment.
    ///
    /// A Topic Name may not be empty, but its root segment will be if the string starts with `/`.
    pub fn root(&self) -> &'a str {
        self.0.first().expect("BUG: TopicName may not be empty").0
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
    #[error("topic name cannot be empty; if a topic alias is used, it must have been resolved")]
    TopicEmpty,
}

#[cfg(test)]
mod tests {
    use super::filter::Filter;
    use super::{FilterTrieMultiMap, TopicName};

    #[derive(Eq, PartialEq, Debug)]
    struct NoClone<T>(T);

    #[test]
    fn insert_remove_by_filter() {
        let filter: Filter = "foo/bar".parse().unwrap();
        let mut trie = FilterTrieMultiMap::new();
        trie.insert(filter.clone(), 0, NoClone(0));

        assert_eq!(trie.remove_by_filter(&filter, &0).map(|it| it.0), Some(0));

        let values = [
            ("foo", 0),
            ("foo/bar", 0),
            ("foo/baz", 0),
            ("foo/#", 0),
            ("foo/baz", 1),
            ("#", 2),
            ("/", 0),
        ];

        for (idx, &(filter, key)) in values.iter().enumerate() {
            trie.insert(filter.parse().unwrap(), key, NoClone(idx as i32 + 1));
        }

        for (idx, &(filter, key)) in values.iter().enumerate() {
            dbg!(idx, &trie);

            assert_eq!(
                trie.remove_by_filter(&filter.parse().unwrap(), &key)
                    .map(|it| it.0),
                Some(idx as i32 + 1)
            );
        }
    }
    #[test]
    fn insert_remove_by_id() {
        let filter: Filter = "foo/bar".parse().unwrap();
        let mut trie = FilterTrieMultiMap::new();
        trie.insert(filter.clone(), 0, NoClone(0));

        assert_eq!(trie.remove_by_filter(&filter, &0).map(|it| it.0), Some(0));

        let values = [
            ("foo", 0),
            ("foo/bar", 0),
            ("foo/baz", 0),
            ("foo/#", 0),
            ("foo/baz", 1),
            ("#", 2),
            ("/", 0),
        ];

        // Test insert and remove_by_place
        let places: Vec<_> = values
            .iter()
            .enumerate()
            .map(|(idx, &(filter, key))| {
                trie.insert(filter.parse().unwrap(), key, NoClone(idx as i32 + 1))
                    .0
            })
            .collect();

        for (idx, (&(_filter, key), place)) in values.iter().zip(places).enumerate() {
            assert_eq!(
                trie.remove_by_id(place, &key).map(|it| it.0),
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
            ("/", 0),
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

        // https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901245
        // > because the single-level wildcard matches only a single level, “sport/+” does not match “sport” but it does match “sport/”.
        expect_test::expect![[r##"
            [
                "#",
                "/",
                "/+",
            ]
        "##]]
        .assert_debug_eq(&matches_sorted(&trie, "/"));
    }

    #[test]
    fn topic_name_parse() {
        // Succeeds
        expect_test::expect![[r#"
            Ok(
                TopicName(
                    [
                        NameToken(
                            "foo",
                        ),
                    ],
                ),
            )
        "#]]
        .assert_debug_eq(&TopicName::parse("foo"));

        expect_test::expect![[r#"
            Ok(
                TopicName(
                    [
                        NameToken(
                            "foo",
                        ),
                        NameToken(
                            "bar",
                        ),
                    ],
                ),
            )
        "#]]
        .assert_debug_eq(&TopicName::parse("foo/bar"));

        expect_test::expect![[r#"
            Ok(
                TopicName(
                    [
                        NameToken(
                            "",
                        ),
                        NameToken(
                            "",
                        ),
                        NameToken(
                            "",
                        ),
                        NameToken(
                            "",
                        ),
                    ],
                ),
            )
        "#]]
        .assert_debug_eq(&TopicName::parse("///"));

        expect_test::expect![[r#"
            Ok(
                TopicName(
                    [
                        NameToken(
                            "",
                        ),
                        NameToken(
                            "",
                        ),
                    ],
                ),
            )
        "#]]
        .assert_debug_eq(&TopicName::parse("/"));

        // Fails
        expect_test::expect![[r#"
            Err(
                TopicEmpty,
            )
        "#]]
        .assert_debug_eq(&TopicName::parse(""));

        expect_test::expect![[r#"
            Err(
                UnexpectedCharacter {
                    ch: '#',
                    idx: 0,
                },
            )
        "#]]
        .assert_debug_eq(&TopicName::parse("#"));

        expect_test::expect![[r#"
            Err(
                UnexpectedCharacter {
                    ch: '+',
                    idx: 0,
                },
            )
        "#]]
        .assert_debug_eq(&TopicName::parse("+"));

        expect_test::expect![[r#"
            Err(
                UnexpectedCharacter {
                    ch: '\0',
                    idx: 7,
                },
            )
        "#]]
        .assert_debug_eq(&TopicName::parse("foo/bar\0"));
    }

    #[test]
    fn topic_name_display() {
        let topics = ["/", "/foo/bar", "foo", "foo/bar", "foo/bar/", "///"];

        for topic in topics {
            assert_eq!(
                TopicName::parse(topic)
                    .unwrap_or_else(|e| panic!("error parsing topic {topic:?}: {e:?}"))
                    .to_string(),
                topic
            );
        }
    }
}
