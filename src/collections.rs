pub use std::collections::hash_map;
pub use std::collections::HashMap;
pub use std::collections::HashSet;

/// Re-export of [`std::collections::HashMap`] using [`fnv::FnvBuildHasher`].
pub type FnvHashMap<K, V> = std::collections::HashMap<K, V, fnv::FnvBuildHasher>;

/// Re-export of [`std::collections::HashSet`] using [`fnv::FnvBuildHasher`].
pub type FnvHashSet<T> = std::collections::HashSet<T, fnv::FnvBuildHasher>;

/// Creates an empty [`HashMap`] with the specified capacity.
#[inline]
pub fn with_capacity<K, V>(capacity: usize) -> HashMap<K, V> {
    HashMap::with_capacity(capacity)
}
