use std::borrow::Borrow;
use std::cmp::Ordering;
use std::str::FromStr;

#[cfg(feature = "arbitrary")]
use arbitrary::Arbitrary;

#[derive(Clone, Debug)]
pub struct Filter {
    // TODO: `TopicName` could use the same structure
    /// The unadulterated filter string. Must not be empty.
    string: Box<str>,
    /// The byte indices (indexes) of level separators (`/`) in the string.
    ///
    /// The filter tokens fall between these indices, with their lengths being the difference
    /// between sequential indices. The start of the first token and the end of the last token
    /// are implied by the bounds of the string itself.
    ///
    /// An empty array indicates that `string` represents just a single token.
    ///
    /// For example, an array length of 3 indicates the presence of 4 tokens:
    /// ```ignore
    /// string[..indices[0]]
    /// string[indices[0] + 1 .. indices[1]]
    /// string[indices[1] + 1 .. indices[2]]
    /// string[indices[2] + 1 ..]
    /// ```
    // This could even be a bitvector if we wanted.
    separator_indices: Box<[usize]>,
    // We don't need to store the `leaf_kind` because we can just look at the end of the string.
}

impl From<&'_ super::TopicName<'_>> for Filter {
    fn from(value: &'_ super::TopicName<'_>) -> Self {
        Self::from_tokens(&value.0, LeafKind::Exact)
    }
}

#[cfg(feature = "arbitrary")]
impl<'a> Arbitrary<'a> for Filter {
    fn arbitrary(u: &mut arbitrary::Unstructured<'a>) -> arbitrary::Result<Self> {
        let leaf_kind: LeafKind = u.arbitrary()?;

        let tokens_in: Vec<FilterToken<&'a str>> = u.arbitrary()?;

        if tokens_in.is_empty() {
            return Ok(Self {
                string: match leaf_kind {
                    LeafKind::Exact => "+",
                    LeafKind::Any => "#",
                }
                .into(),
                separator_indices: [].into(),
            });
        }

        Ok(Self::from_tokens(&tokens_in, leaf_kind))
    }
}

impl FromStr for Filter {
    type Err = FilterParseError;

    fn from_str(string: &str) -> Result<Self, Self::Err> {
        if string.is_empty() {
            return Err(FilterParseError::EmptyFilter);
        }

        // Count the number of levels first so we can pre-allocate `tokens`.
        //
        // The hope is that this pays off with less traffic to the allocator.
        let token_count = string.split('/').count();

        if token_count == 0 {
            return Ok(Filter {
                string: string.into(),
                separator_indices: [].into(),
            });
        }

        let mut separator_indices = Vec::with_capacity(token_count);

        let mut token_start = 0;

        let mut tokens = string.split('/');

        while let Some(token) = tokens.next() {
            // `#` is not a valid `FilterToken`, it's counted in `LeafKind` instead.
            if token == "#" {
                // `#` cannot appear in the middle of a filter.
                if tokens.next().is_some() {
                    return Err(FilterParseError::InvalidWildcard);
                }
            } else {
                // Validate the token
                FilterToken::try_from(token)?;
            }

            // This will be the index of the `/`.
            let separator_index = token_start + token.len();

            separator_indices.push(separator_index);

            // Put the next `token_start` after the `/`.
            token_start = separator_index + 1;
        }

        Ok(Filter {
            string: string.into(),
            separator_indices: separator_indices.into(),
        })
    }
}

impl Filter {
    // Takes `impl Borrow<str>` so we can pass `NameToken` or `FilterToken`.
    // Takes a slice to optimally pre-allocate our capacity (important for fuzzing).
    /// Construct a `Filter` from a list of tokens and a `LeafKind`.
    ///
    /// ### Note
    /// This method assumes every token in `tokens` is valid according to `FilterToken::try_from()`.
    /// The behavior of `Filter` and the rest of the `trie` module is unspecified otherwise,
    /// i.e. may panic or do weird things, but not trigger UB.
    // Since this method is private, we can place the burden on the code calling this method
    // to ensure the tokens are valid.
    //
    // This avoids redundant validation, which would slow things down.
    //
    // We _could_ validate in `if cfg!(debug_assertions) { ... }`,
    // but one concern is the performance of fuzzing where that flag is enabled for good reasons.
    fn from_tokens(tokens: &[impl Borrow<str>], leaf_kind: LeafKind) -> Self {
        if tokens.is_empty() {
            assert_eq!(leaf_kind, LeafKind::Exact, "filter cannot be empty");

            return Self {
                string: "#".into(),
                separator_indices: [].into(),
            };
        }

        // If we allocate the exact capacity needed up-front,
        // converting to `Box<str>` should be a no-op.
        let mut string = String::with_capacity(
            // Count the number of separators (`/`)
            tokens.len().saturating_sub(1) +
                    // Sum the lengths of the tokens.
                    tokens.iter().map(|t| t.borrow().len()).sum::<usize>() +
                    // Count the characters in the leaf
                    match leaf_kind {
                        // Filter ends with a literal or + which was already counted.
                        LeafKind::Exact => 0,
                        // Filter ends with `/#` which does not appear in the tokens.
                        LeafKind::Any => 2,
                    },
        );

        // Same here.
        let mut separator_indices = Vec::with_capacity(
            tokens.len().saturating_sub(1) +
                // Include the separator for the trailing `/#` since it's not passed in as a token.
                match leaf_kind {
                    LeafKind::Exact => 0,
                    LeafKind::Any => 1,
                },
        );

        for tokens in tokens.windows(2) {
            // `.windows()` will always yield non-empty slices.
            string.push_str(tokens[0].borrow());

            // If another token follows this one, we know to push a separator.
            //
            // We can't use `string.is_empty()` for this
            // because we could have an empty string as our first token.
            if tokens.len() > 1 {
                separator_indices.push(string.len());
                string.push('/');
            }
        }

        if leaf_kind == LeafKind::Any {
            separator_indices.push(string.len());
            string.push_str("/#");
        }

        Self {
            string: string.into(),
            separator_indices: separator_indices.into(),
        }
    }

    pub(super) fn tokens(&self) -> impl DoubleEndedIterator<Item = FilterToken<&'_ str>> {
        self.token_indices().map(|(_i, token)| token)
    }

    fn token_indices(&self) -> impl DoubleEndedIterator<Item = (usize, FilterToken<&'_ str>)> {
        let mut token_start = 0;

        self.separator_indices
            .iter()
            .filter_map(move |&token_end| {
                // Bypass `TryFrom` validation for `FilterToken`.
                let token = match &self.string[token_start..token_end] {
                    // `#` doesn't count as a token in our scheme.
                    "#" => return None,
                    "+" => FilterToken::WildPlus,
                    lit => FilterToken::Literal(lit),
                };

                let ret = (token_start, token);

                token_start = token_end + 1;

                Some(ret)
            })
            .chain(
                // Append our last token if it's not `#`
                self.separator_indices.last().and_then(|last_index| {
                    let last_token_start = last_index + 1;

                    match self.string.get(last_token_start..)? {
                        "#" => None,
                        "+" => Some((last_token_start, FilterToken::WildPlus)),
                        lit => Some((last_token_start, FilterToken::Literal(lit))),
                    }
                }),
            )
    }

    pub fn as_str(&self) -> &str {
        &self.string
    }

    /// Get the root of this filter, if it's a literal string and not a wildcard.
    pub fn root_literal(&self) -> Option<&str> {
        self.tokens().next().and_then(|t| t.into_literal())
    }

    /// Returns `true` if this filter contains no wildcards.
    pub fn is_exact(&self) -> bool {
        self.leaf_kind() == LeafKind::Exact && self.tokens().any(|t| t.is_wildcard())
    }

    /// If this is filter is exact (contains no wildcards), return `Ok(self.as_str())`.
    ///
    /// Otherwise, return the literal (wildcard-free) prefix of the filter.
    pub fn exact_or_prefix(&self) -> Result<&str, &str> {
        // `+` should be covered in the `for` loop
        // (slicing required to satisfy the == operator)
        if &self.string[..] == "#" {
            return Err("");
        }

        for (index, token) in self.token_indices() {
            if token.is_wildcard() {
                return Err(&self.string[..index]);
            }
        }

        // If the string ends in `/#` then the literal prefix is just everything before that.
        if let Some(trimmed) = self.string.strip_suffix("/#") {
            return Err(trimmed);
        }

        // Otherwise, it's a literal string.
        Ok(&self.string)
    }

    /// Returns `true` if this filter matches the given topic, `false` otherwise.
    ///
    /// The topic doesn't need to be well-formed.
    pub fn matches_topic(&self, topic: &str) -> bool {
        // Empty topics are not allowed in the spec.
        if topic.is_empty() {
            return false;
        }

        let mut tokens = self.tokens();

        for level in topic.split('/') {
            match tokens.next() {
                Some(token) => {
                    if !token.matches(level) {
                        return false;
                    }
                }
                // There's more levels in the topic than in the filter.
                // This is only a match if the filter ends with `#` (multi-level wildcard).
                None => return self.leaf_kind() == LeafKind::Any,
            }
        }

        // Require the whole filter to be consumed
        tokens.next().is_none()
    }

    pub(super) fn leaf_kind(&self) -> LeafKind {
        if self.string.ends_with('#') {
            LeafKind::Any
        } else {
            LeafKind::Exact
        }
    }
}

impl Borrow<str> for Filter {
    fn borrow(&self) -> &str {
        self.as_str()
    }
}

#[derive(thiserror::Error, Debug)]
#[cfg_attr(test, derive(PartialEq, Eq))]
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

impl<'a> From<InvalidTokenError<'a>> for FilterParseError {
    fn from(e: InvalidTokenError<'a>) -> Self {
        FilterParseError::InvalidToken {
            token: e.token.into(),
            pos: e.pos,
            ch: e.ch,
        }
    }
}

#[derive(Clone, Debug)]
// Defaulted type param so the `trie` module can continue using it like normal.
pub(super) enum FilterToken<Lit = Box<str>> {
    // Note: `#` doesn't count as a token in our scheme since it only appears at the end of a filter,
    // so it's covered by `LeafKind` instead.
    //
    // This way, the rest of the `trie` module doesn't have to have code to handle the possibility
    // of an invalid filter having `#` somewhere in the middle.
    /// A `+` (any on this level) wildcard.
    WildPlus,
    /// `text`
    Literal(Lit),
}

#[derive(thiserror::Error, Debug)]
#[error("token {token} contains an invalid character starting at {pos} (`{ch}`)")]
pub(super) struct InvalidTokenError<'a> {
    pub token: &'a str,
    pub pos: usize,
    pub ch: char,
}

impl<'a> TryFrom<&'a str> for FilterToken<&'a str> {
    type Error = InvalidTokenError<'a>;

    fn try_from(token: &'a str) -> Result<Self, Self::Error> {
        if token == "+" {
            return Ok(Self::WildPlus);
        }

        if let Some((idx, ch)) = token
            .char_indices()
            .find(|it| matches!(it.1, '#' | '+' | '\0'))
        {
            return Err(InvalidTokenError {
                token,
                pos: idx,
                ch,
            });
        }

        Ok(Self::Literal(token))
    }
}

#[cfg(feature = "arbitrary")]
impl<'a> Arbitrary<'a> for FilterToken<&'a str> {
    fn arbitrary(u: &mut arbitrary::Unstructured<'a>) -> arbitrary::Result<Self> {
        let b: u8 = match u.arbitrary() {
            Ok(it) => it,
            Err(arbitrary::Error::NotEnoughData) => return Ok(FilterToken::WildPlus),
            Err(e) => return Err(e),
        };

        match b & 1 {
            0 => Ok(
                <FilterToken<&str> as TryFrom<&str>>::try_from(u.arbitrary()?)
                    .or_else(|e| FilterToken::try_from(&e.token[..e.pos]))
                    .expect("BUG: FilterToken should have accepted the trimmed string"),
            ),
            1 => Ok(FilterToken::WildPlus),
            _ => unreachable!(),
        }
    }
}

impl<Lit: Borrow<str>> FilterToken<Lit> {
    fn as_str(&self) -> &str {
        match self {
            Self::Literal(lit) => lit.borrow(),
            Self::WildPlus => "+",
        }
    }

    pub(super) fn matches(&self, name: impl Borrow<str>) -> bool {
        match self {
            FilterToken::Literal(lit) => lit.borrow() == name.borrow(),
            FilterToken::WildPlus => true,
        }
    }

    pub fn into_literal(self) -> Option<Lit> {
        match self {
            FilterToken::Literal(lit) => Some(lit),
            _ => None,
        }
    }

    pub fn is_wildcard(&self) -> bool {
        matches!(self, Self::WildPlus)
    }

    pub fn boxed(&self) -> FilterToken<Box<str>> {
        match self {
            Self::Literal(lit) => FilterToken::Literal(lit.borrow().into()),
            Self::WildPlus => FilterToken::WildPlus,
        }
    }

    pub fn cmp_strings<Lit2: Borrow<str>>(&self, other: &FilterToken<Lit2>) -> Ordering {
        self.as_str().cmp(other.as_str())
    }
}

impl<Lit: Borrow<str>> Borrow<str> for FilterToken<Lit> {
    fn borrow(&self) -> &str {
        self.as_str()
    }
}

impl<Lit1: Borrow<str>, Lit2: Borrow<str>> PartialEq<FilterToken<Lit2>> for FilterToken<Lit1> {
    fn eq(&self, other: &FilterToken<Lit2>) -> bool {
        self.as_str() == other.as_str()
    }
}

impl<Lit: Borrow<str>> Eq for FilterToken<Lit> {}

impl<Lit1: Borrow<str>, Lit2: Borrow<str>> PartialOrd<FilterToken<Lit2>> for FilterToken<Lit1> {
    fn partial_cmp(&self, other: &FilterToken<Lit2>) -> Option<Ordering> {
        Some(self.cmp_strings(other))
    }
}

impl<Lit: Borrow<str>> Ord for FilterToken<Lit> {
    fn cmp(&self, other: &Self) -> Ordering {
        self.cmp_strings(other)
    }
}

#[derive(Copy, Clone, Eq, PartialEq, Debug, Hash)]
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

#[cfg(test)]
mod tests {
    use super::{Filter, FilterParseError, LeafKind};

    use std::str::FromStr;

    // NOTE: some assertions below assume this list has all topics starting with `/` grouped together.
    const TEST_TOPICS: &[&str] = &[
        "foo",
        "foo/",
        "foo//",
        "foo///",
        "foo/bar",
        "foo/bar/",
        "foo/bar/baz",
        "foo/bar/baz/",
        "/",
        "/foo",
        "/foo/",
        "/foo/bar",
        "/foo/bar/",
        "/foo/bar/baz",
        "/foo/bar/baz/",
        // Topic levels can be empty
        "//",
        "/bar/",
        "//baz",
        "///",
        "/bar//",
        "//baz/",
        "/foo//",
        "//bar/",
        "///baz",
        "////",
        "/foo///",
        "//bar//",
        "///baz/",
    ];

    #[test]
    fn matches_topic() {
        /// Iterate through all the topics in `TEST_TOPICS`
        /// and assert whether the given filter matches or not.
        #[track_caller]
        fn test_filter(filter: &str, matches: &[&str]) {
            let parsed: Filter = filter
                .parse()
                .unwrap_or_else(|e| panic!("filter {filter:?} failed to parse: {e:?}"));

            for topic in TEST_TOPICS {
                if matches.contains(topic) {
                    assert!(
                        parsed.matches_topic(topic),
                        "filter {filter:?} should match topic {topic:?} but doesn't (expected matches: {matches:?})"
                    );
                } else {
                    assert!(
                        !parsed.matches_topic(topic),
                        "filter {filter:?} shouldn't match topic {topic:?} but does  (expected matches: {matches:?})"
                    );
                }
            }
        }

        // Test exact matches (any topic as a filter should match itself)
        for &filter in TEST_TOPICS {
            test_filter(filter, &[filter]);
        }

        // Single-level wildcards
        test_filter("+/bar/baz", &["foo/bar/baz"]);
        test_filter("foo/+/baz", &["foo/bar/baz"]);
        // `+` should match an empty level created by a trailing `/`
        test_filter("foo/bar/+", &["foo/bar/", "foo/bar/baz"]);

        test_filter("+/bar/baz/", &["foo/bar/baz/"]);
        test_filter("foo/+/baz/", &["foo/bar/baz/"]);
        test_filter("foo/bar/+/", &["foo/bar/baz/"]);
        test_filter("foo/bar/baz/+", &["foo/bar/baz/"]);

        test_filter("/+/bar/baz", &["/foo/bar/baz"]);
        test_filter("/foo/+/baz", &["/foo/bar/baz"]);
        test_filter("/foo/bar/+", &["/foo/bar/", "/foo/bar/baz"]);

        test_filter("/+/bar/baz/", &["/foo/bar/baz/"]);
        test_filter("/foo/+/baz/", &["/foo/bar/baz/"]);
        test_filter("/foo/bar/+/", &["/foo/bar/baz/"]);
        test_filter("/foo/bar/baz/+", &["/foo/bar/baz/"]);

        test_filter("+", &["foo"]);
        test_filter("+/", &["/", "foo/"]);
        test_filter("foo/+", &["foo/", "foo/bar"]);
        test_filter("+/+", &["/", "foo/", "/foo", "foo/bar"]);
        test_filter("+/+/", &["/foo/", "foo/bar/", "//", "foo//", "/bar/"]);
        test_filter("foo/+/+", &["foo/bar/", "foo/bar/baz", "foo//"]);
        test_filter(
            "+/+/+",
            &[
                "/foo/",
                "/foo/bar",
                "foo/bar/",
                "foo/bar/baz",
                "//",
                "foo//",
                "/bar/",
                "//baz",
            ],
        );
        test_filter(
            "+/+/+/+",
            &[
                "/foo/bar/",
                "/foo/bar/baz",
                "foo/bar/baz/",
                "///",
                "foo///",
                "/bar//",
                "//baz/",
                "/foo//",
                "//bar/",
                "///baz",
            ],
        );

        test_filter("/+", &["/", "/foo"]);
        test_filter("/foo/+", &["/foo/", "/foo/bar"]);
        test_filter("/+/+", &["/foo/", "/foo/bar", "//", "/bar/", "//baz"]);
        test_filter("/foo/+/+", &["/foo/bar/", "/foo/bar/baz", "/foo//"]);
        test_filter(
            "/+/+/+",
            &[
                "/foo/bar/",
                "/foo/bar/baz",
                "///",
                "/foo//",
                "/bar//",
                "//bar/",
                "//baz/",
                "///baz",
            ],
        );
        test_filter(
            "/+/+/+/+",
            &["/foo/bar/baz/", "////", "/foo///", "//bar//", "///baz/"],
        );

        // Multi-level wildcard
        test_filter("#", TEST_TOPICS);
        test_filter(
            "foo/#",
            &[
                // The multi-level wildcard represents the parent and any number of child levels.
                "foo",
                "foo/",
                "foo//",
                "foo/bar",
                "foo/bar/",
                "foo///",
                "foo/bar/baz",
                "foo/bar/baz/",
            ],
        );
        test_filter(
            "foo/bar/#",
            &["foo/bar", "foo/bar/", "foo/bar/baz", "foo/bar/baz/"],
        );
        test_filter("foo/bar/baz/#", &["foo/bar/baz", "foo/bar/baz/"]);

        let slash_topics_start = TEST_TOPICS
            .iter()
            .position(|topic| topic.starts_with('/'))
            .expect("TEST_TOPICS should contain a set of topics prefixed with `/`");

        test_filter("/#", &TEST_TOPICS[slash_topics_start..]);

        test_filter(
            "/foo/#",
            &[
                "/foo",
                "/foo/",
                "/foo//",
                "/foo///",
                "/foo/bar",
                "/foo/bar/",
                "/foo/bar/baz",
                "/foo/bar/baz/",
            ],
        );

        test_filter(
            "/foo/bar/#",
            &["/foo/bar", "/foo/bar/", "/foo/bar/baz", "/foo/bar/baz/"],
        );

        test_filter("/foo/bar/baz/#", &["/foo/bar/baz", "/foo/bar/baz/"]);
    }

    #[test]
    fn getters() {
        // All literal-only filters should have themselves as prefixes.
        for filter in TEST_TOPICS {
            let parsed: Filter = filter
                .parse()
                .unwrap_or_else(|e| panic!("filter {filter:?} failed to parse: {e:?}"));

            assert_eq!(
                parsed.root_literal(),
                // All `TEST_TOPICS` start with either `/` or `foo`
                Some(if parsed.as_str().starts_with('/') {
                    ""
                } else {
                    "foo"
                }),
                "incorrect root literal for filter {filter:?}"
            );

            // Literal filters are always `LeafKind::Exact`
            assert_eq!(
                parsed.leaf_kind(),
                LeafKind::Exact,
                "incorrect leaf kind for filter {filter:?}"
            );

            assert_eq!(
                parsed.exact_or_prefix(),
                Ok(*filter),
                "expected the literal prefix of filter {filter:?} to equal itself"
            );
        }

        let test_filters = [
            // (filter, prefix, root_literal, leaf_kind)
            ("+", "", None, LeafKind::Exact),
            ("#", "", None, LeafKind::Any),
            ("+/+", "", None, LeafKind::Exact),
            ("+/#", "", None, LeafKind::Any),
            // `#` matches the parent level, so our literal prefix shouldn't include the final `/`
            ("foo/#", "foo", Some("foo"), LeafKind::Any),
            ("foo/+", "foo/", Some("foo"), LeafKind::Exact),
            ("foo/+/baz", "foo/", Some("foo"), LeafKind::Exact),
            ("foo/bar/+", "foo/bar/", Some("foo"), LeafKind::Exact),
            ("foo/bar/#", "foo/bar", Some("foo"), LeafKind::Any),
            ("/", "/", Some(""), LeafKind::Exact),
            ("/+", "/", Some(""), LeafKind::Exact),
            ("/#", "", Some(""), LeafKind::Any),
            ("/foo", "/foo", Some(""), LeafKind::Exact),
            ("/foo/#", "/foo", Some(""), LeafKind::Any),
            ("/foo/+", "/foo/", Some(""), LeafKind::Exact),
            ("/foo/+/baz", "/foo/", Some(""), LeafKind::Exact),
            ("/foo/bar/#", "/foo/bar", Some(""), LeafKind::Any),
            ("/foo/bar/+", "/foo/bar/", Some(""), LeafKind::Exact),
        ];

        for (filter, prefix, root_literal, leaf_kind) in test_filters {
            let parsed: Filter = filter
                .parse()
                .unwrap_or_else(|e| panic!("filter {filter:?} failed to parse: {e:?}"));

            assert_eq!(
                parsed.root_literal(),
                root_literal,
                "incorrect root literal for filter {filter:?}"
            );

            assert_eq!(
                parsed.leaf_kind(),
                leaf_kind,
                "incorrect leaf kind for filter {filter:?}"
            );

            assert_eq!(
                parsed.exact_or_prefix(),
                if filter == prefix {
                    Ok(prefix)
                } else {
                    Err(prefix)
                },
                "incorrect prefix for filter {filter:?}"
            );
        }
    }

    #[test]
    fn rejects_invalid_topics() {
        macro_rules! assert_err {
            ($filter:literal, $err:pat $(, $expr:expr)?) => {
                match Filter::from_str($filter) {
                    Ok(filter) => {
                        panic!(
                            "expected filter {:?} to be rejected, got {filter:?}",
                            $filter
                        );
                    }
                    Err($err) $(if $expr)? => (),
                    Err(other) => {
                        panic!(
                            "expected error pattern:\n{}\ngot:\n{other:?}",
                            stringify!($err)
                        )
                    }
                }
            };
        }

        assert_err!("", FilterParseError::EmptyFilter);
        assert_err!(
            "\0",
            FilterParseError::InvalidToken {
                token,
                pos: 0,
                ch: '\0',
            },
            token == "\0"
        );
        assert_err!("#/bar", FilterParseError::InvalidWildcard);
    }
}
