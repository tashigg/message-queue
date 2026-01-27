use std::collections::hash_map::RandomState;

pub use hashbrown::hash_map;
pub use hashbrown::hash_set;
pub use hashbrown::Equivalent;

/// Re-export of [`hashbrown::HashMap`] using [`RandomState`] ("DefaultHasher").
pub type HashMap<K, V> = hashbrown::HashMap<K, V, RandomState>;

/// Re-export of [`hashbrown::HashSet`] using [`RandomState`] ("DefaultHasher").
pub type HashSet<T> = hashbrown::HashSet<T, RandomState>;

/// Re-export of [`hashbrown::HashMap`] using [`fnv::FnvBuildHasher`].
pub type FnvHashMap<K, V> = hashbrown::HashMap<K, V, fnv::FnvBuildHasher>;

/// Re-export of [`hashbrown::HashSet`] using [`fnv::FnvBuildHasher`].
pub type FnvHashSet<T> = hashbrown::HashSet<T, fnv::FnvBuildHasher>;

/// Creates an empty [`HashMap`] with the specified capacity.
#[inline]
pub fn with_capacity<K, V>(capacity: usize) -> HashMap<K, V> {
    HashMap::with_capacity_and_hasher(capacity, Default::default())
}
