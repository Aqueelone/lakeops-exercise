//! How the API is used. These pass on the starter code. Add your own tests next to
//! this file; we also run a hidden test suite against your submission.
//! The Part 3 tests are in `waiting.rs`.

use std::sync::Arc;

use admission::{AcquireError, ClusterConfig, ClusterPool, PoolConfig};

fn pool(clusters: &[(&str, u64)]) -> Arc<ClusterPool> {
    ClusterPool::new(
        PoolConfig::default().group(
            "g",
            clusters
                .iter()
                .map(|(name, max)| ClusterConfig::new(*name, *max))
                .collect(),
        ),
    )
}

#[test]
fn permit_can_cross_threads() {
    fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<admission::Permit>();
    assert_send_sync::<ClusterPool>();
}

#[test]
fn unknown_group() {
    let pool = pool(&[("a", 1)]);
    assert_eq!(
        pool.try_acquire("nope").err(),
        Some(AcquireError::UnknownGroup)
    );
}

#[test]
fn admits_up_to_capacity_then_busy() {
    let pool = pool(&[("a", 2), ("b", 1)]);
    let permits: Vec<_> = (0..3).map(|_| pool.try_acquire("g").unwrap()).collect();
    assert_eq!(pool.try_acquire("g").err(), Some(AcquireError::Busy));
    assert_eq!(pool.running("g", "a"), Some(2));
    assert_eq!(pool.running("g", "b"), Some(1));

    permits.into_iter().for_each(|p| p.release());
    assert_eq!(pool.running("g", "a"), Some(0));
    assert_eq!(pool.admissions("g", "a"), 2);
    assert_eq!(pool.admissions("g", "b"), 1);
}
