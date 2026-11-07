use admission::{ClusterConfig, ClusterPool, PoolConfig};
use std::sync::Arc;
use std::time::Duration;
use tokio::time::Instant;

fn pool(max_running: u64) -> Arc<ClusterPool> {
    ClusterPool::new(PoolConfig::default().group(
        "analytics",
        vec![ClusterConfig::new("trino-1", max_running)],
    ))
}

#[tokio::test]
async fn edge_case_cancellation_storm_maintains_metrics() {
    let pool = pool(0); // Нульова місткість, всі одразу йдуть у чергу

    let mut tasks = vec![];
    // Створюємо 1000 клієнтів, які стають у чергу
    for _ in 0..1000 {
        let p = Arc::clone(&pool);
        tasks.push(tokio::spawn(async move {
            let _ = p
                .acquire_timeout("analytics", Duration::from_secs(60))
                .await;
        }));
    }

    // Даємо Tokio час поставити всіх у чергу
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(pool.queued("analytics"), 1000);

    // Влаштовуємо Cancellation Storm: жорстко вбиваємо половину задач
    for task in tasks.iter().take(500) {
        task.abort();
    }

    // Чекаємо, поки Drop для WaitGuard відпрацює
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Перевіряємо, що lock-free лічильник ідеально зійшовся
    assert_eq!(
        pool.queued("analytics"),
        500,
        "Лічильник черги зламався після масового скасування"
    );
}

#[tokio::test]
async fn edge_case_reload_preserves_waiting_clients() {
    let pool = pool(1);
    let _held = pool.try_acquire("analytics").unwrap(); // Забираємо єдиний слот

    let p_clone = Arc::clone(&pool);
    let waiter = tokio::spawn(async move {
        p_clone
            .acquire_timeout("analytics", Duration::from_secs(5))
            .await
    });

    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(pool.queued("analytics"), 1);

    // Адмін робить гаряче перезавантаження конфігурації!
    let new_config =
        PoolConfig::default().group("analytics", vec![ClusterConfig::new("trino-1", 1)]);
    pool.reload(new_config);

    // Переконуємось, що черга не загубилась при підміні ArcSwap
    assert_eq!(pool.queued("analytics"), 1);

    // Звільняємо слот
    _held.release();

    // Клієнт має успішно прокинутись і забрати слот, незважаючи на те, що конфіг змінився
    let permit = waiter
        .await
        .unwrap()
        .expect("Клієнт відвалився по таймауту замість пробудження!");
    assert_eq!(permit.cluster(), "trino-1");
}

#[tokio::test]
async fn edge_case_dynamic_capacity_increase_wakes_waiters() {
    let pool = pool(0); // Стартуємо з 0 слотів

    let p_clone = Arc::clone(&pool);
    let waiter = tokio::spawn(async move {
        // Клієнт готовий чекати 5 секунд
        p_clone
            .acquire_timeout("analytics", Duration::from_secs(5))
            .await
    });

    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(pool.queued("analytics"), 1);

    let start = Instant::now();

    // Адмін на льоту додає слоти!
    pool.set_max_running("analytics", "trino-1", 10);

    // ВАЖЛИВО: Щоб цей тест пройшов, нам треба додати логіку пробудження (notify_all)
    // всередину set_max_running та reload!

    let result = waiter.await.unwrap();
    assert!(
        result.is_ok(),
        "Клієнт не прокинувся після збільшення місткості!"
    );
    assert!(
        start.elapsed() < Duration::from_millis(100),
        "Клієнт чекав занадто довго!"
    );
}
