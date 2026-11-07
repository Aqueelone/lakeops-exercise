use admission::{ClusterConfig, ClusterPool, PoolConfig};
use std::sync::Arc;
use std::time::Duration;

#[test]
fn test_inc_1_wedged_gateway_permit_leak() {
    let config = PoolConfig::default().group("trino", vec![ClusterConfig::new("trino-3", 10)]);
    let pool = ClusterPool::new(config);

    // ІМІТАЦІЯ СПЛЕСКУ ТРАФІКУ ТА СКАСУВАННЯ
    for _ in 0..10 {
        let permit = pool
            .try_acquire("trino")
            .expect("Має успішно отримати слот");
        drop(permit);
    }

    // НАСТУПНИЙ РАНОК
    let result = pool.try_acquire("trino");

    // Використовуємо .is_ok() замість {:?} для `result`, оскільки `Permit` не реалізує `Debug`.
    // Якщо result є Err, ми можемо роздрукувати саму помилку (AcquireError реалізує Debug).
    assert!(
        result.is_ok(),
        "INC-1 Reproducer (Wedged): Шлюз застряг! Повернуто помилку: {:?}. Витік слотів!",
        result.err()
    );
}

#[test]
fn test_inc_1_cap_exceeded_race_condition() {
    use std::sync::Barrier;
    use std::thread;

    let max_running_limit = 100;
    let config = PoolConfig::default().group(
        "trino",
        vec![ClusterConfig::new("trino-3", max_running_limit)],
    );
    let pool = ClusterPool::new(config);

    let num_threads = 200;
    let barrier = Arc::new(Barrier::new(num_threads));

    let mut handles = vec![];
    for _ in 0..num_threads {
        let pool = Arc::clone(&pool);
        let barrier = Arc::clone(&barrier);

        handles.push(thread::spawn(move || {
            barrier.wait();
            pool.try_acquire("trino")
        }));
    }

    let mut successful_acquires = 0;
    let mut permits = vec![];

    for handle in handles {
        if let Ok(Ok(permit)) = handle.join() {
            successful_acquires += 1;
            permits.push(permit);
        }
    }

    assert!(
        successful_acquires <= max_running_limit as usize,
        "INC-1 Reproducer (Cap Exceeded): Пропущено {} запитів при ліміті {}! Стан гонитви у try_increment.",
        successful_acquires,
        max_running_limit
    );
}

#[test]
fn test_inc_2_capacity_cut_debug_panic() {
    let config = PoolConfig::default().group("trino", vec![ClusterConfig::new("trino-debug", 40)]);
    let pool = ClusterPool::new(config);

    // Імітуємо повне завантаження кластера (40 запитів)
    let mut permits = vec![];
    for _ in 0..40 {
        permits.push(
            pool.try_acquire("trino")
                .expect("Має успішно отримати слот"),
        );
    }

    // Оператор знижує ліміт з 40 до 10 під час навантаження
    pool.set_max_running("trino", "trino-debug", 10);

    // Запускаємо ще один запит. У debug збірці старого коду (available: 10 - 40)
    // це викличе panic! через overflow.
    // Ми використовуємо std::panic::catch_unwind, щоб тест міг зловити паніку.
    let result = std::panic::catch_unwind(|| {
        let _ = pool.try_acquire("trino");
    });

    // На старому коді result буде Err(_) (паніка відбулася).
    // Після виправлення на saturating_sub, result буде Ok(Err(Busy)) і тест пройде.
    assert!(
        result.is_ok(),
        "INC-2 Reproducer (Panic): Шлюз впав із панікою (underflow) при зниженні ліміту! Змініть `max_running() - running()` на `saturating_sub`."
    );
}

#[test]
fn test_inc_2_capacity_cut_release_wedged() {
    let config = PoolConfig::default().group(
        "trino",
        vec![
            ClusterConfig::new("trino-overloaded", 40),
            ClusterConfig::new("trino-idle", 40), // Ідеально вільний кластер
        ],
    );
    let pool = ClusterPool::new(config);

    // Заповнюємо ТІЛЬКИ перший кластер (імітуємо локальне перевантаження до зниження ліміту)
    let mut overloaded_permits = vec![];
    for _ in 0..40 {
        // Оскільки на початку обидва вільні, try_acquire балансуватиме.
        // Щоб гарантовано забити перший кластер, ми просто вичерпуємо всі 80 слотів
        overloaded_permits.push(pool.try_acquire("trino").unwrap());
    }

    // А тепер звільняємо ДРУГИЙ кластер, залишаючи ПЕРШИЙ забитим
    let mut idle_permits = vec![];
    while let Ok(permit) = pool.try_acquire("trino") {
        if permit.cluster() == "trino-idle" {
            idle_permits.push(permit); // Зберігаємо, щоб звільнити
        } else {
            overloaded_permits.push(permit);
        }
    }
    drop(idle_permits); // trino-idle тепер повністю вільний (40 слотів)

    // Оператор знижує ліміт ПЕРШОГО кластера з 40 до 10
    pool.set_max_running("trino", "trino-overloaded", 10);

    // На старому коді у Release збірці:
    // trino-overloaded.available() = 10u64.wrapping_sub(40u64) = 18_446_744_073_709_551_586
    // trino-idle.available() = 40
    //
    // `pick` вибере trino-overloaded (бо 18 квінтильйонів > 40).
    // `try_increment` на trino-overloaded поверне false (бо 40 >= 10).
    // `try_acquire` поверне Err(Busy)! Хоча trino-idle має 40 вільних місць!

    let result = pool.try_acquire("trino");

    assert!(
        result.is_ok(),
        "INC-2 Reproducer (Release Wedged): Група відхиляє запити (Busy), хоча інші кластери вільні! Перевантажений кластер діє як 'чорна діра' через overflow."
    );
}

#[test]
fn test_inc_3_busy_with_free_capacity_toctou() {
    use std::sync::Barrier;
    use std::thread;

    // Встановлюємо групу з трьома кластерами.
    // Сумарна місткість: 10 + 10 + 10 = 30 слотів.
    let config = PoolConfig::default().group(
        "trino",
        vec![
            ClusterConfig::new("trino-1", 10),
            ClusterConfig::new("trino-2", 10),
            ClusterConfig::new("trino-3", 10),
        ],
    );
    let pool = ClusterPool::new(config);

    // Імітуємо нагрузочний тест: 30 потоків стартують одночасно.
    // Оскільки загальна місткість 30, УСІ 30 запитів мають бути успішними,
    // і дашборд не повинен показувати Busy (відмови).
    let num_threads = 30;
    let barrier = Arc::new(Barrier::new(num_threads));

    let mut handles = vec![];
    for _ in 0..num_threads {
        let pool = Arc::clone(&pool);
        let barrier = Arc::clone(&barrier);

        handles.push(thread::spawn(move || {
            barrier.wait();
            pool.try_acquire("trino")
        }));
    }

    let mut successful_acquires = 0;
    let mut failed_busy = 0;
    let mut permits = vec![]; // Зберігаємо, щоб слоти не звільнялися достроково

    for handle in handles {
        match handle.join().unwrap() {
            Ok(permit) => {
                successful_acquires += 1;
                permits.push(permit);
            }
            Err(admission::AcquireError::Busy) => {
                failed_busy += 1;
            }
            _ => panic!("Неочікувана помилка!"),
        }
    }

    // На старому коді (без fallback):
    // 30 потоків викликають `c.available()`. Всі бачать, що trino-1 найкращий.
    // Вони всі намагаються інкрементувати trino-1. Перші 10 забирають слоти.
    // Наступні 20 отримують false від try_increment(trino-1) і миттєво повертають Err(Busy),
    // ХОЧА trino-2 і trino-3 мають по 10 вільних слотів!

    assert_eq!(
        failed_busy, 0,
        "INC-3 Reproducer (TOCTOU): Клієнти отримали Busy ({failed_busy} разів), хоча в групі були вільні слоти! Шлюз здався після першої невдачі."
    );

    assert_eq!(
        successful_acquires, 30,
        "Мали зайняти всі 30 слотів рівномірно."
    );
}

#[tokio::test]
async fn test_inc_4_slots_never_recovered_timeout() {
    // Встановлюємо місткість кластера у 5 слотів
    let config = PoolConfig::default().group("trino", vec![ClusterConfig::new("trino-1", 5)]);
    let pool = ClusterPool::new(config);

    // Імітуємо аварію Trino (Trino outage):
    // 5 клієнтів роблять запити, отримують слоти, але Trino "висить".
    // Клієнти відвалюються по таймауту.
    for _ in 0..5 {
        let pool_clone = Arc::clone(&pool);

        // Імітація обробника запиту (Request Handler)
        let handler = async move {
            let _permit = pool_clone.try_acquire("trino").expect("Отримали слот");

            // Імітуємо довге очікування відповіді від Trino, яке ніколи не завершується
            tokio::time::sleep(Duration::from_secs(60)).await;

            // В нормальних умовах `_permit` був би дропнутий тут.
        };

        // Клієнт не витримує і розриває з'єднання через 10 мілісекунд
        let result = tokio::time::timeout(Duration::from_millis(10), handler).await;

        // Переконуємося, що стався саме таймаут (Future було скасовано)
        assert!(result.is_err(), "Очікувався таймаут клієнта");
    }

    // Аварія закінчилася. Trino знову працює.
    // Перевіряємо, чи відновилася місткість кластера.
    let result = pool.try_acquire("trino");

    assert!(
        result.is_ok(),
        "INC-4 Reproducer: Слоти не відновилися після таймауту клієнтів! Місткість назавжди втрачена. Потрібен impl Drop for Permit."
    );
}
