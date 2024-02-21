pub struct Filter {
    pub(super) tokens: Vec<FilterToken>,
    pub(super) leaf_kind: LeafKind,
}

#[derive(PartialEq, PartialOrd, Eq, Ord)]
pub(super) enum FilterToken {
    /// `text`
    Literal(Box<str>),
    /// A `+` (any on this level) wildcard.
    WildPlus,
}

#[derive(Copy, Clone)]
pub(super) enum LeafKind {
    /// No wildcard.
    Exact,
    /// A `#` wildcard found.
    Any,
}

impl LeafKind {
    /// Returns `true` if the leaf kind is [`Any`].
    ///
    /// [`Any`]: LeafKind::Any
    #[must_use]
    pub(super) fn is_any(&self) -> bool {
        matches!(self, Self::Any)
    }

    /// Returns `true` if the leaf kind is [`Exact`].
    ///
    /// [`Exact`]: LeafKind::Exact
    #[must_use]
    pub(super) fn is_exact(&self) -> bool {
        matches!(self, Self::Exact)
    }
}
