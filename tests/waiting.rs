use std::sync::Arc;
use std::time::{Duration, Instant};

use admission::{AcquireError, ClusterConfig, ClusterPool, PoolConfig};

fn pool(max_running: u64) -> Arc<ClusterPool> {
    ClusterPool::new(PoolConfig::default().group("g", vec![ClusterConfig::new("a", max_running)]))
}

async fn wait_until(what: &str, cond: impl Fn() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(2);
    while !cond() {
        assert!(Instant::now() < deadline, "timed out waiting for: {what}");
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
}

#[tokio::test]
async fn free_slot_is_returned_immediately() {
    let pool = pool(1);
    let start = Instant::now();
    let permit = pool
        .acquire_timeout("g", Duration::from_secs(5))
        .await
        .unwrap();
    assert!(start.elapsed() < Duration::from_millis(50));
    assert_eq!(permit.cluster(), "a");
}

#[tokio::test]
async fn unknown_group_fails_without_waiting() {
    let pool = pool(1);
    let start = Instant::now();
    let err = pool
        .acquire_timeout("nope", Duration::from_secs(5))
        .await
        .err();
    assert_eq!(err, Some(AcquireError::UnknownGroup));
    assert!(start.elapsed() < Duration::from_millis(50));
}

#[tokio::test]
async fn waiter_gets_the_slot_when_it_is_released() {
    let pool = pool(1);
    let held = pool.try_acquire("g").unwrap();

    let waiter = {
        let pool = Arc::clone(&pool);
        tokio::spawn(async move { pool.acquire_timeout("g", Duration::from_secs(5)).await })
    };
    wait_until("one queued waiter", || pool.queued("g") == 1).await;

    let released_at = Instant::now();
    held.release();
    let permit = waiter.await.unwrap().unwrap();
    assert!(
        released_at.elapsed() < Duration::from_millis(100),
        "waiter was not woken promptly"
    );
    assert_eq!(permit.cluster(), "a");
    assert_eq!(pool.queued("g"), 0);
}

#[tokio::test]
async fn waiter_times_out() {
    let pool = pool(1);
    let _held = pool.try_acquire("g").unwrap();

    let start = Instant::now();
    let err = pool
        .acquire_timeout("g", Duration::from_millis(50))
        .await
        .err();
    assert_eq!(err, Some(AcquireError::Timeout));
    assert!(start.elapsed() >= Duration::from_millis(50));
    assert!(start.elapsed() < Duration::from_millis(500));
    assert_eq!(pool.queued("g"), 0);
}

#[tokio::test]
async fn cancelled_waiter_leaves_the_queue() {
    let pool = pool(1);
    let _held = pool.try_acquire("g").unwrap();

    let waiter = {
        let pool = Arc::clone(&pool);
        tokio::spawn(async move { pool.acquire_timeout("g", Duration::from_secs(60)).await })
    };
    wait_until("one queued waiter", || pool.queued("g") == 1).await;
    waiter.abort();
    let _ = waiter.await;
    assert_eq!(pool.queued("g"), 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn no_lost_wakeups_under_load() {
    // 2 slots, 200 callers that each hold a slot briefly. Every caller must get in
    // well before its 10s deadline; a lost wakeup shows up as a Timeout.
    let pool = pool(2);
    let tasks: Vec<_> = (0..200)
        .map(|_| {
            let pool = Arc::clone(&pool);
            tokio::spawn(async move {
                let permit = pool.acquire_timeout("g", Duration::from_secs(10)).await?;
                tokio::time::sleep(Duration::from_millis(1)).await;
                drop(permit);
                Ok::<_, AcquireError>(())
            })
        })
        .collect();

    for task in tasks {
        task.await.unwrap().unwrap();
    }
    assert_eq!(pool.running("g", "a"), Some(0));
    assert_eq!(pool.queued("g"), 0);
}
