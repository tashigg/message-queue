use std::str::FromStr;

use super::NameToken;

#[derive(Clone, Debug)]
pub struct Filter {
    pub(super) tokens: Vec<FilterToken>,
    pub(super) leaf_kind: LeafKind,
}

impl FromStr for Filter {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s.is_empty() {
            todo!("empty err")
        }

        let mut tokens = Vec::new();
        let mut leaf_kind = LeafKind::Exact;

        for token in s.split('/') {
            // we have another filter after an empty filter
            if leaf_kind == LeafKind::Any {
                todo!("# not end of filter err");
            }

            match token {
                "+" => tokens.push(FilterToken::WildPlus),
                "#" => leaf_kind = LeafKind::Any,
                _ if token.contains(|c| matches!(c, '#' | '+' | '\0')) => todo!("invalid char err"),
                _ => tokens.push(FilterToken::Literal(token.to_owned().into_boxed_str())),
            }
        }

        Ok(Self { tokens, leaf_kind })
    }
}

#[derive(PartialEq, PartialOrd, Eq, Ord, Clone, Debug)]
pub(super) enum FilterToken {
    /// `text`
    Literal(Box<str>),
    /// A `+` (any on this level) wildcard.
    WildPlus,
}

impl FilterToken {
    pub(super) fn matches(&self, name: NameToken<'_>) -> bool {
        match self {
            FilterToken::Literal(lit) => &**lit == name.0,
            FilterToken::WildPlus => true,
        }
    }
}

#[derive(Copy, Clone, Eq, PartialEq, Debug)]
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
}
