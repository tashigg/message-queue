use std::borrow::Borrow;
use std::fmt::{Debug, Display, Formatter};
use std::hash::{Hash, Hasher};
use std::num::NonZeroU8;
use std::ops::Deref;
use std::str::FromStr;

use rand::distributions::{Alphanumeric, Distribution};
use rand::Rng;
use crate::collections::Equivalent;

/// The maximum length a `ClientId` is allowed to be.
///
/// The specification allows implementations to reject client IDs longer than this.
pub const MAX_LEN: usize = 23;

/// A container for MQTT client IDs that fits entirely on the stack and is trivially copyable.
#[derive(Copy, Clone, PartialEq, Eq)]
pub struct ClientId {
    // A `String` is 3 pointers, which turns out to be 24 bytes on 64-bit platforms.
    // Coincidentally, the MQTT spec only requires implementations to support client IDs
    // with up to 23 bytes, so this fits perfectly in the same space
    // with another byte for the length.
    //
    // We could fit this into a smaller space using a compressed encoding
    // (64 characters can be compacted into a 6 bit encoding), but that space would likely be wasted
    // in padding for alignment in the containing structure anyway,
    // and it wouldn't be trivially convertible to `&str`.
    /// SAFETY: must be in the range `1 ..= 23`.
    len: NonZeroU8,
    /// SAFETY: `bytes` must *always* be valid UTF-8.
    bytes: [u8; MAX_LEN],
}

impl ClientId {
    /// Generate a random client ID of the given length.
    ///
    /// ### Panics
    /// If `length` is not in the range `1 ..= MAX_LEN`.
    // APIs in the `rand` crate all use signatures like this, likely to avoid a whole separate
    // monomorphization for when you call it with `&mut impl Rng` vs `impl Rng`.
    pub fn generate<R: Rng + ?Sized>(rng: &mut R, length: usize) -> Self {
        assert!(
            (1..=MAX_LEN).contains(&length),
            "{length} not in the range `1 ..= {MAX_LEN}`"
        );

        // We could use `MaybeUninit` for `bytes` but `ClientId` is not constructed very frequently
        // and that'd just be another safety thing to worry about.
        let mut bytes = [0u8; MAX_LEN];

        // There isn't really a more efficient way. `.sample_iter()` would just do this internally.
        for b in &mut bytes[..length] {
            // Other implementations may generate non-compliant IDs (*COUGH*mqttjs*COUGH*),
            // but we shouldn't.
            //
            // SAFETY: we need to be certain that this generates values in a valid range.
            //
            // `Alphanumeric.sample(impl Rng)` is trustworthy because code in the `rand` crate has
            // the final say, and they trust it in `unsafe` code in the `DistString` impl
            // to do pretty much this exact same thing.
            //
            // However, `(impl Rng).sample(Alphanumeric)` can't be trusted because
            // the trait impl can override the method and return a value outside the sample range.
            *b = Alphanumeric.sample(rng);
        }

        Self {
            // SAFETY: `length` must be in the range `1 ..= MAX_LEN` which is checked above.
            // A truncating cast thus cannot overflow.
            len: NonZeroU8::new(length as u8).expect("BUG: `length` should be nonzero"),
            bytes,
        }
    }

    pub fn from_bytes(byte_slice: &[u8]) -> Result<Self, ParseError> {
        let len_usize = byte_slice.len();

        if len_usize > MAX_LEN {
            return Err(ParseError::InvalidLength(len_usize));
        }

        // SAFETY: Truncating cast cannot overflow if it's not larger than `MAX_LEN`.
        let len = NonZeroU8::new(len_usize as u8).ok_or(ParseError::Empty)?;

        let mut bytes = [0u8; MAX_LEN];

        for (position, (&byte, byte_out)) in byte_slice.iter().zip(&mut bytes).enumerate() {
            // http://docs.oasis-open.org/mqtt/mqtt/v3.1.1/os/mqtt-v3.1.1-os.html#_Toc398718031
            //
            // The Server MUST allow ClientIds which are between 1 and 23 UTF-8 encoded bytes in length,
            // and that contain only the characters
            // "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ" [MQTT-3.1.3-5].
            //
            // The Server MAY allow ClientId’s that contain more than 23 encoded bytes.
            // The Server MAY allow ClientId’s that contain characters not included in the list given above.
            // -----------------
            // In fact, `MQTT.js` generates client IDs with underscores by default:
            // https://www.npmjs.com/package/mqtt#client
            // `clientId: 'mqttjs_' + Math.random().toString(16).substr(2, 8)`
            if !matches!(
                byte,
                b'A'..= b'Z' | b'a' ..= b'z' | b'0' ..= b'9' | b'-' | b'_',
            ) {
                return Err(ParseError::InvalidByte { byte, position });
            }

            // SAFETY: we only accept one-byte (ASCII) characters,
            // so we don't have to worry about multibyte characters.
            *byte_out = byte;
        }

        Ok(Self { len, bytes })
    }

    pub fn as_bytes(&self) -> &[u8] {
        let len = self.len.get() as usize;

        debug_assert!(
            (1..=MAX_LEN).contains(&len),
            "BUG: `len` out of bounds: {len}"
        );

        // SAFETY: `self.len` is always in-bounds.
        unsafe { self.bytes.get_unchecked(..len) }
    }

    pub fn as_str(&self) -> &str {
        let bytes = self.as_bytes();

        if cfg!(debug_assertions) {
            return std::str::from_utf8(bytes).unwrap_or_else(|e| {
                panic!("BUG: `bytes` is not valid UTF-8: {bytes:?}\nError: {e:?}")
            });
        }

        unsafe {
            // SAFETY: `bytes` being valid UTF-8 is an invariant of the type.
            // Having to check this every time we convert to `str` is likely nontrivial overhead.
            std::str::from_utf8_unchecked(bytes)
        }
    }
}

impl Deref for ClientId {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        self.as_str()
    }
}

impl AsRef<str> for ClientId {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl Borrow<str> for ClientId {
    fn borrow(&self) -> &str {
        self.as_str()
    }
}

impl Debug for ClientId {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("ClientId").field(&self.as_str()).finish()
    }
}

impl Display for ClientId {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        Display::fmt(self.as_str(), f)
    }
}

/// Guaranteed to hash the same as `self.as_str()` to allow for `HashMap` lookups with `&str`.
impl Hash for ClientId {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.as_str().hash(state)
    }
}

impl PartialEq<str> for ClientId {
    fn eq(&self, other: &str) -> bool {
        self.as_str().eq(other)
    }
}

impl PartialEq<ClientId> for str {
    fn eq(&self, other: &ClientId) -> bool {
        self.eq(other.as_str())
    }
}


impl Equivalent<str> for ClientId {
    fn equivalent(&self, key: &str) -> bool {
        self == key
    }
}

impl From<ClientId> for String {
    fn from(value: ClientId) -> Self {
        value.as_str().into()
    }
}

#[derive(thiserror::Error, Debug)]
pub enum ParseError {
    #[error("client ID cannot be empty")]
    Empty,
    #[error("expected a client ID length between 1 and 23, got {0}")]
    InvalidLength(usize),
    #[error("invalid byte {byte:02X} in client ID at position {position}")]
    InvalidByte { byte: u8, position: usize },
}

impl FromStr for ClientId {
    type Err = ParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::from_bytes(s.as_bytes())
    }
}

impl TryFrom<&'_ [u8]> for ClientId {
    type Error = ParseError;

    fn try_from(value: &'_ [u8]) -> Result<Self, Self::Error> {
        Self::from_bytes(value)
    }
}

#[cfg(test)]
mod tests {
    use std::mem;
    use std::str::FromStr;

    use expect_test::expect;

    use super::ClientId;

    #[test]
    fn option_is_same_size() {
        // `ClientId::len` is a `NonZeroU8` so the compiler should be able to use that
        // for niche-filling optimization.
        assert_eq!(
            mem::size_of::<ClientId>(),
            mem::size_of::<Option<ClientId>>()
        );
    }

    #[test]
    fn from_str() {
        expect![[r#"
            Err(
                Empty,
            )
        "#]]
        .assert_debug_eq(&ClientId::from_str(""));

        // All `Ok()` variants indirectly test `.as_str()` as well.
        expect![[r#"
            Ok(
                ClientId(
                    "0",
                ),
            )
        "#]]
        .assert_debug_eq(&ClientId::from_str("0"));

        expect![[r#"
            Ok(
                ClientId(
                    "AsDf1234",
                ),
            )
        "#]]
        .assert_debug_eq(&ClientId::from_str("AsDf1234"));

        // An ACTUAL client ID generated by MQTT.js.
        expect![[r#"
            Ok(
                ClientId(
                    "mqttjs_684dbee5",
                ),
            )
        "#]]
        .assert_debug_eq(&ClientId::from_str("mqttjs_684dbee5"));

        // https://www.satisfice.com/blog/archives/22
        expect![[r#"
            Err(
                InvalidByte {
                    byte: 42,
                    position: 1,
                },
            )
        "#]]
        .assert_debug_eq(&ClientId::from_str("2*4*6*8*11*14*17*20*23*"));

        expect![[r#"
            Err(
                InvalidLength(
                    35,
                ),
            )
        "#]]
        .assert_debug_eq(&ClientId::from_str("2*4*6*8*11*14*17*20*23*26*29*32*35*"));

        expect![[r#"
            Ok(
                ClientId(
                    "2-4-6-8-11-14-17-20-23-",
                ),
            )
        "#]]
        .assert_debug_eq(&ClientId::from_str("2-4-6-8-11-14-17-20-23-"));
    }
}
