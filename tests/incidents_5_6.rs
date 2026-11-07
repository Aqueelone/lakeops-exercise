use std::sync::Arc;
use std::time::Duration;
use admission::{AcquireError, ClusterConfig, ClusterPool, PoolConfig};

fn pool(max_running: u64) -> Arc<ClusterPool> {
    ClusterPool::new(PoolConfig::default().group("analytics", vec![ClusterConfig::new("trino-1", max_running)]))
}

#[tokio::test]
async fn test_inc_5_reconcile_drifts_under_load() {
    let pool = pool(10);

    // 1. Видаємо 2 слоти
    let _p1 = pool.try_acquire("analytics").unwrap();
    let _p2 = pool.try_acquire("analytics").unwrap();
    assert_eq!(pool.running("analytics", "trino-1"), Some(2));

    // 2. Запускаємо reconcile у фоні.
    // Імітуємо повільний рушій, який каже "у мене 2 запити", але відповідає 50 мс.
    let pool_clone = Arc::clone(&pool);
    let reconcile_task = tokio::spawn(async move {
        pool_clone.reconcile("analytics", "trino-1", || async {
            tokio::time::sleep(Duration::from_millis(50)).await;
            2
        }).await;
    });

    // 3. ПОКИ reconcile чекає на відповідь, шлюз під навантаженням приймає ще 3 запити
    let _p3 = pool.try_acquire("analytics").unwrap();
    let _p4 = pool.try_acquire("analytics").unwrap();
    let _p5 = pool.try_acquire("analytics").unwrap();

    // Зараз реально виконується 5 запитів
    assert_eq!(pool.running("analytics", "trino-1"), Some(5));

    // 4. Чекаємо завершення reconcile
    reconcile_task.await.unwrap();

    // 5. Перевіряємо INC-5: сліпий store() скине лічильник назад на 2, загубивши p3, p4, p5!
    assert_eq!(
        pool.running("analytics", "trino-1"),
        Some(5),
        "INC-5: reconcile затер паралельні зміни і зіпсував лічильник!"
    );
}

#[tokio::test]
async fn test_inc_6_reload_resets_counters() {
    let pool = pool(10);

    // 1. Займаємо 2 слоти
    let _p1 = pool.try_acquire("analytics").unwrap();
    let _p2 = pool.try_acquire("analytics").unwrap();

    assert_eq!(pool.running("analytics", "trino-1"), Some(2));
    assert_eq!(pool.admissions("analytics", "trino-1"), 2);

    // 2. Робимо гаряче перезавантаження (можливо, додали новий кластер trino-2)
    let new_config = PoolConfig::default()
        .group("analytics", vec![
            ClusterConfig::new("trino-1", 10),
            ClusterConfig::new("trino-2", 10),
        ]);
    pool.reload(new_config);

    // 3. Перевіряємо INC-6: лічильники мають пережити reload!
    assert_eq!(
        pool.running("analytics", "trino-1"),
        Some(2),
        "INC-6: лічильник running обнулився після reload!"
    );
    assert_eq!(
        pool.admissions("analytics", "trino-1"),
        2,
        "INC-6: лічильник admissions обнулився після reload!"
    );

    // 4. Переконуємося, що ми не можемо перевищити ліміт (10)
    let mut extra_permits = vec![];
    for _ in 0..8 {
        extra_permits.push(pool.try_acquire("analytics").unwrap());
    }

    // 11-й запит має відхилитися. Якщо лічильник скинувся, він пройде.
    assert_eq!(
        pool.try_acquire("analytics").err(),
        Some(AcquireError::Busy),
        "INC-6: кластер прийняв більше запитів, ніж дозволено, через скидання місткості!"
    );
}