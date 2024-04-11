use std::cmp;
use std::num::NonZeroU16;
use std::time::Duration;

/// Container for MQTT Keep Alive intervals.
#[derive(Copy, Clone, Debug, Default, PartialEq, Eq)]
pub struct KeepAlive(Option<NonZeroU16>);

impl KeepAlive {
    /// Wrap a number of seconds as a Keep Alive interval.
    ///
    /// A value of zero means no Keep Alive interval.
    pub fn from_seconds(seconds: u16) -> KeepAlive {
        Self(NonZeroU16::new(seconds))
    }

    pub fn is_zero(self) -> bool {
        self.0.is_none()
    }

    /// If this Keep Alive is nonzero, return it multiplied by 1.5 for use as a timeout.
    ///
    /// > If the Keep Alive value is non-zero and the Server does not receive an MQTT Control Packet
    /// > from the Client within one and a half times the Keep Alive time period, it MUST close the
    /// > Network Connection to the Client as if the network had failed [MQTT-3.1.2-22].
    pub fn as_timeout(self) -> Option<Duration> {
        self.0
            // `.mul_f32()` may panic if the result overflows or is not finite,
            // but we don't have to worry about that here as it will never be large enough.
            .map(|seconds| Duration::from_secs(seconds.get().into()).mul_f32(1.5))
    }

    /// Apply the maximum Keep Alive to this value.
    ///
    /// If `self` is nonzero and `max` is nonzero, the new interval is the minimum of the two.
    /// If `self` is zero and `max` is nonzero, the new interval is `max`.
    /// Otherwise, returns `self`.
    pub fn with_max(self, max: Self) -> Self {
        // Couldn't think of a more concise way to express this that wouldn't obscure the intent.
        Self(match (self.0, max.0) {
            (Some(keep_alive), Some(max_keep_alive)) => Some(cmp::min(keep_alive, max_keep_alive)),
            (None, Some(max_keep_alive)) => Some(max_keep_alive),
            (Some(keep_alive), None) => Some(keep_alive),
            (None, None) => None,
        })
    }

    /// Return the number of seconds in this Keep Alive interval.
    ///
    /// If zero, no Keep Alive interval was set.
    pub fn as_seconds(self) -> u16 {
        self.0.map_or(0, |seconds| seconds.get())
    }
}
