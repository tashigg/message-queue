use std::str::FromStr;

#[cfg(feature = "arbitrary")]
use arbitrary::Arbitrary;

use super::NameToken;

#[derive(Clone, Debug)]
pub struct Filter {
    pub(super) tokens: Vec<FilterToken>,
    pub(super) leaf_kind: LeafKind,
}

impl From<&'_ super::TopicName<'_>> for Filter {
    fn from(value: &'_ super::TopicName<'_>) -> Self {
        Self {
            tokens: value
                .0
                .iter()
                .map(|it| FilterToken::Literal(it.0.to_owned().into_boxed_str()))
                .collect(),
            leaf_kind: LeafKind::Exact,
        }
    }
}

#[cfg(feature = "arbitrary")]
impl<'a> Arbitrary<'a> for Filter {
    fn arbitrary(u: &mut arbitrary::Unstructured<'a>) -> arbitrary::Result<Self> {
        let tokens: Vec<_> = u.arbitrary()?;

        let leaf_kind = if tokens.len() != 0 {
            u.arbitrary()?
        } else {
            LeafKind::Any
        };

        Ok(Self { tokens, leaf_kind })
    }
}

impl FromStr for Filter {
    type Err = FilterParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s.is_empty() {
            return Err(FilterParseError::EmptyFilter);
        }

        let mut tokens = Vec::new();
        let mut leaf_kind = LeafKind::Exact;

        for token in s.split('/') {
            // we have another filter after an empty filter
            if leaf_kind == LeafKind::Any {
                return Err(FilterParseError::InvalidWildcard);
            }

            match token {
                "+" => tokens.push(FilterToken::WildPlus),
                "#" => leaf_kind = LeafKind::Any,
                _ => {
                    if let Some((idx, ch)) = token
                        .char_indices()
                        .find(|it| matches!(it.1, '#' | '+' | '\0'))
                    {
                        return Err(FilterParseError::InvalidToken {
                            token: token.to_owned(),
                            pos: idx,
                            ch,
                        });
                    }

                    tokens.push(FilterToken::Literal(token.to_owned().into_boxed_str()));
                }
            }
        }

        Ok(Self { tokens, leaf_kind })
    }
}

#[derive(thiserror::Error, Debug)]
pub enum FilterParseError {
    /// Filter must not be empty.
    #[error("filter must not be empty")]
    EmptyFilter,

    /// `token` contains an invalid character (`ch`) at `pos`.
    #[error("{token} contains an invalid character starting at {pos} (`{ch}`)")]
    InvalidToken { token: String, pos: usize, ch: char },

    /// Found a `#` wildcard and it wasn't the end of the filter.
    #[error("filter contains a `#` wildcard that isn't trailing")]
    InvalidWildcard,
}

#[derive(PartialEq, PartialOrd, Eq, Ord, Clone, Debug)]
pub(super) enum FilterToken {
    /// `text`
    Literal(Box<str>),
    /// A `+` (any on this level) wildcard.
    WildPlus,
}

#[cfg(feature = "arbitrary")]
impl<'a> Arbitrary<'a> for FilterToken {
    fn arbitrary(u: &mut arbitrary::Unstructured<'a>) -> arbitrary::Result<Self> {
        let b: u8 = match u.arbitrary() {
            Ok(it) => it,
            Err(arbitrary::Error::NotEnoughData) => return Ok(FilterToken::WildPlus),
            Err(e) => return Err(e),
        };

        match b & 1 {
            0 => Ok(FilterToken::Literal(u.arbitrary()?)),
            1 => Ok(FilterToken::WildPlus),
            _ => unreachable!(),
        }
    }
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

// manual impl to use less bytes of input (default uses a u32)
// less input = smaller, smaller = smaller corpus, faster.
#[cfg(feature = "arbitrary")]
impl<'a> arbitrary::Arbitrary<'a> for LeafKind {
    fn arbitrary(u: &mut arbitrary::Unstructured<'a>) -> arbitrary::Result<Self> {
        let b: u8 = u.arbitrary()?;

        match b & 1 {
            0 => Ok(LeafKind::Exact),
            1 => Ok(LeafKind::Any),
            _ => unreachable!(),
        }
    }

    fn size_hint(_depth: usize) -> (usize, Option<usize>) {
        (1, Some(1))
    }
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
