use admission::{AcquireError, ClusterConfig, ClusterPool, PoolConfig};
use std::sync::Arc;
use std::time::Duration;

fn pool(max_running: u64) -> Arc<ClusterPool> {
    ClusterPool::new(PoolConfig::default().group(
        "analytics",
        vec![ClusterConfig::new("trino-1", max_running)],
    ))
}

#[tokio::test]
async fn test_inc_5_reconcile_drifts_under_load() {
    let pool = pool(10);

    let _p1 = pool.try_acquire("analytics").unwrap();
    let _p2 = pool.try_acquire("analytics").unwrap();
    assert_eq!(pool.running("analytics", "trino-1"), Some(2));

    // Використовуємо канал, щоб синхронізувати тест із фоновим завданням
    let (tx, rx) = tokio::sync::oneshot::channel();
    let pool_clone = Arc::clone(&pool);

    let reconcile_task = tokio::spawn(async move {
        pool_clone
            .reconcile("analytics", "trino-1", || async {
                // Сигналізуємо основному потоку, що fetch почався (тобто baseline = 2 вже прочитано!)
                let _ = tx.send(());
                tokio::time::sleep(Duration::from_millis(50)).await;
                2
            })
            .await;
    });

    // Чекаємо, поки reconcile ТОЧНО прочитає baseline
    rx.await.unwrap();

    // ТЕПЕР імітуємо навантаження під час довгого fetch
    let _p3 = pool.try_acquire("analytics").unwrap();
    let _p4 = pool.try_acquire("analytics").unwrap();
    let _p5 = pool.try_acquire("analytics").unwrap();

    assert_eq!(pool.running("analytics", "trino-1"), Some(5));

    reconcile_task.await.unwrap();

    // Перевіряємо, що наша компенсуюча транзакція врятувала паралельний трафік
    assert_eq!(
        pool.running("analytics", "trino-1"),
        Some(5),
        "INC-5: reconcile затер паралельні зміни і зіпсував лічильник!"
    );
}

#[tokio::test]
async fn test_inc_6_reload_resets_counters() {
    let pool = pool(10);

    let _p1 = pool.try_acquire("analytics").unwrap();
    let _p2 = pool.try_acquire("analytics").unwrap();

    assert_eq!(pool.running("analytics", "trino-1"), Some(2));
    assert_eq!(pool.admissions("analytics", "trino-1"), 2);

    // Перезавантажуємо конфіг, залишаючи строго trino-1, щоб загальна місткість групи залишилась 10
    let new_config =
        PoolConfig::default().group("analytics", vec![ClusterConfig::new("trino-1", 10)]);
    pool.reload(new_config);

    assert_eq!(
        pool.running("analytics", "trino-1"),
        Some(2),
        "INC-6: лічильник running обнулився після reload!"
    );

    let mut extra_permits = vec![];
    for _ in 0..8 {
        extra_permits.push(pool.try_acquire("analytics").unwrap());
    }

    // 11-й запит має відхилитися
    assert_eq!(
        pool.try_acquire("analytics").err(),
        Some(AcquireError::Busy),
        "INC-6: кластер прийняв більше запитів, ніж дозволено, через скидання місткості!"
    );
}
