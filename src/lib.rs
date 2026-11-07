//! Cluster admission control for a SQL gateway.
//!
//! A gateway fronts several *groups* of engine clusters (Trino, StarRocks, ...).
//! Every incoming query must be admitted to one cluster of its target group
//! before it is forwarded, and each cluster accepts at most `max_running`
//! concurrent queries. The gateway handles thousands of queries per second,
//! so `try_acquire` sits on the hot path of every request.
//!
//! See README.md for the tasks.

use arc_swap::ArcSwap;
use std::collections::HashMap;
use std::future::Future;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Notify;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClusterConfig {
    pub name: String,
    pub max_running: u64,
}

impl ClusterConfig {
    pub fn new(name: impl Into<String>, max_running: u64) -> Self {
        Self {
            name: name.into(),
            max_running,
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PoolConfig {
    pub groups: HashMap<String, Vec<ClusterConfig>>,
}

impl PoolConfig {
    pub fn group(mut self, name: impl Into<String>, clusters: Vec<ClusterConfig>) -> Self {
        self.groups.insert(name.into(), clusters);
        self
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AcquireError {
    /// The group does not exist in the current config.
    UnknownGroup,
    /// Every cluster in the group is at capacity.
    Busy,
    /// `acquire_timeout` waited for its full duration without getting a slot.
    Timeout,
}

#[derive(Debug)]
struct ClusterState {
    name: String,
    max_running: AtomicU64,
    running: AtomicU64,
    admissions: AtomicU64, // <-- ДОДАНО
    epoch: AtomicU64,      // <-- ДОДАНО: Захист від гонитви при релоадах
}

impl ClusterState {
    fn new(config: &ClusterConfig) -> Self {
        Self {
            name: config.name.clone(),
            max_running: AtomicU64::new(config.max_running),
            running: AtomicU64::new(0),
            admissions: AtomicU64::new(0), // <-- ДОДАНО
            epoch: AtomicU64::new(0),      // <-- ДОДАНО
        }
    }

    fn running(&self) -> u64 {
        self.running.load(Ordering::Relaxed)
    }

    fn max_running(&self) -> u64 {
        self.max_running.load(Ordering::Relaxed)
    }

    fn available(&self) -> u64 {
        self.max_running().saturating_sub(self.running())
    }

    // 1. ОПТИМІЗАЦІЯ АТОМАРНИХ ОПЕРАЦІЙ (lock xadd замість lock cmpxchg)
    fn try_increment(&self) -> bool {
        // Миттєвий інкремент без циклів (найшвидша атомарна операція)
        let prev = self.running.fetch_add(1, Ordering::Relaxed);

        if prev >= self.max_running() {
            // Rollback (відкат), якщо перевищили ліміт
            self.running.fetch_sub(1, Ordering::Relaxed);
            false
        } else {
            true
        }
    }

    fn decrement(&self) {
        self.running.fetch_sub(1, Ordering::Relaxed);
    }
}

#[derive(Debug, Clone)]
struct Candidate {
    // name: String, -- not use now, but could be used in next scopes
    available: u64,
}

/// Least-loaded selection: the candidate with the most free slots wins.
/// -- for next use when it will be required
#[allow(dead_code)]
fn pick(candidates: &[Candidate]) -> Option<usize> {
    candidates
        .iter()
        .enumerate()
        .max_by_key(|(_, c)| c.available)
        .map(|(i, _)| i)
}

pub struct ClusterPool {
    groups: ArcSwap<Vec<Arc<GroupState>>>,
}

impl ClusterPool {
    pub fn new(config: PoolConfig) -> Arc<Self> {
        let mut initial = Vec::new();
        for (name, clusters) in &config.groups {
            let states = clusters
                .iter()
                .map(|c| Arc::new(ClusterState::new(c)))
                .collect();
            initial.push(Arc::new(GroupState {
                name: name.clone(),
                clusters: states,
                notify: Arc::new(Notify::new()),
                queued: Arc::new(AtomicU64::new(0)),
            }));
        }
        Arc::new(Self {
            groups: ArcSwap::from_pointee(initial),
        })
    }

    fn find(&self, group: &str, cluster: &str) -> Option<Arc<ClusterState>> {
        self.groups
            .load()
            .iter()
            .find(|g| g.name == group)?
            .clusters
            .iter()
            .find(|c| c.name == cluster)
            .cloned()
    }

    pub fn try_acquire(self: &Arc<Self>, group: &str) -> Result<Permit, AcquireError> {
        let guard = self.groups.load();
        let group_state = guard
            .iter()
            .find(|g| g.name == group)
            .ok_or(AcquireError::UnknownGroup)?;
        self.acquire_from_group(group_state)
    }

    // Виносимо логіку вибору кластера (P2C) в окремий метод
    fn acquire_from_group(&self, group: &GroupState) -> Result<Permit, AcquireError> {
        let len = group.clusters.len();
        if len == 0 {
            return Err(AcquireError::Busy);
        }

        for _ in 0..3 {
            let state = if len == 1 {
                &group.clusters[0]
            } else {
                let i = fastrand::usize(0..len);
                let mut j = fastrand::usize(0..len);
                if i == j {
                    j = (j + 1) % len;
                }
                let a = &group.clusters[i];
                let b = &group.clusters[j];
                if a.available() >= b.available() {
                    a
                } else {
                    b
                }
            };

            if state.available() == 0 {
                continue;
            }

            if state.try_increment() {
                state.admissions.fetch_add(1, Ordering::Relaxed);
                return Ok(Permit {
                    state: Arc::clone(state),
                    group_notify: Arc::clone(&group.notify),
                    group_queued: Arc::clone(&group.queued),
                });
            }
        }
        Err(AcquireError::Busy)
    }

    pub async fn acquire_timeout(
        self: &Arc<Self>,
        group: &str,
        wait: Duration,
    ) -> Result<Permit, AcquireError> {
        // Клонуємо `Arc<GroupState>`, щоб не тримати `ArcSwapGuard` через `.await` (це заблокує оновлення конфіга)
        let group_state = {
            let guard = self.groups.load();
            guard
                .iter()
                .find(|g| g.name == group)
                .cloned()
                .ok_or(AcquireError::UnknownGroup)?
        };

        // 1. Швидкий шлях: можливо, слот уже вільний
        if let Ok(permit) = self.acquire_from_group(&group_state) {
            return Ok(permit);
        }

        // 2. Стаємо в чергу
        group_state.queued.fetch_add(1, Ordering::Relaxed);

        // RAII Guard: гарантує, що лічильник зменшиться, навіть якщо Future буде скасовано (cancellation)
        struct WaitGuard {
            queued: Arc<AtomicU64>,
        }
        impl Drop for WaitGuard {
            fn drop(&mut self) {
                self.queued.fetch_sub(1, Ordering::Relaxed);
            }
        }
        let _wait_guard = WaitGuard {
            queued: Arc::clone(&group_state.queued),
        };

        // 3. Чекаємо з таймаутом
        let wait_fut = tokio::time::timeout(wait, async {
            loop {
                // ВАЖЛИВО: Отримуємо future `notified` ДО перевірки наявності слота.
                // Це повністю унеможливлює стан гонитви (втрачене пробудження).
                let notified = group_state.notify.notified();

                if let Ok(permit) = self.acquire_from_group(&group_state) {
                    return permit;
                }

                // Засинаємо, чекаючи на notify_one() від чийогось Permit::drop
                notified.await;
            }
        });

        match wait_fut.await {
            Ok(permit) => Ok(permit),
            Err(_) => Err(AcquireError::Timeout),
        }
    }

    /// Current running count for a cluster, or `None` if it is not in the current config.
    pub fn running(&self, group: &str, cluster: &str) -> Option<u64> {
        self.find(group, cluster).map(|c| c.running())
    }

    /// Number of callers currently waiting inside `acquire_timeout` for `group`.
    pub fn queued(&self, group: &str) -> u64 {
        let guard = self.groups.load();
        if let Some(g) = guard.iter().find(|g| g.name == group) {
            g.queued.load(Ordering::Relaxed)
        } else {
            0
        }
    }

    /// Total queries ever admitted to a cluster (a metric scraped by Prometheus).
    pub fn admissions(&self, group: &str, cluster: &str) -> u64 {
        // Знаходимо кластер за допомогою існуючого методу find
        // і читаємо атомарне значення без жодних локів (locks)
        self.find(group, cluster)
            .map(|c| c.admissions.load(Ordering::Relaxed))
            .unwrap_or(0)
    }

    /// Admin API: change a cluster's capacity at runtime.
    /// fixed after fail test: edge_case_dynamic_capacity_increase_wakes_waiters
    pub fn set_max_running(&self, group: &str, cluster: &str, max_running: u64) {
        if let Some(state) = self.find(group, cluster) {
            let old = state.max_running.swap(max_running, Ordering::Relaxed);

            // НОВЕ: Інвалідуємо фонові reconcile, бо місткість змінилася
            state.epoch.fetch_add(1, Ordering::Relaxed);

            // Якщо місткість збільшилася, нам треба розбудити тих, хто висить у черзі!
            if max_running > old {
                let guard = self.groups.load();
                if let Some(g) = guard.iter().find(|g| g.name == group) {
                    // Використовуємо notify_waiters() замість notify_one().
                    // Адже місткість могла зрости на 10, і якщо ми збудимо лише одного,
                    // інші 9 слотів будуть простоювати. Ті, кому слота все одно не вистачить,
                    // просто зроблять спробу і заснуть назад.
                    g.notify.notify_waiters();
                }
            }
        }
    }

    /// Bonus: swap in a new config while queries are in flight.
    /// fixed after fail test: edge_case_dynamic_capacity_increase_wakes_waiters
    pub fn reload(&self, config: PoolConfig) {
        let old_guard = self.groups.load();

        // 1. Збираємо старі групи для перевикористання
        let mut existing_groups = std::collections::HashMap::new();
        for g in old_guard.iter() {
            existing_groups.insert(g.name.clone(), Arc::clone(g));
        }

        let mut new_groups = Vec::new();
        for (name, clusters) in &config.groups {
            let old_group = existing_groups.remove(name);

            let mut old_clusters = std::collections::HashMap::new();
            let (notify, queued) = if let Some(g) = &old_group {
                for c in &g.clusters {
                    old_clusters.insert(c.name.clone(), Arc::clone(c));
                }
                (Arc::clone(&g.notify), Arc::clone(&g.queued))
            } else {
                (Arc::new(Notify::new()), Arc::new(AtomicU64::new(0)))
            };

            let mut states = Vec::new();
            for c in clusters {
                // ФІКС INC-6: Перевикористовуємо існуючий стан!
                // Це зберігає running та admissions лічильники.
                if let Some(old_c) = old_clusters.remove(&c.name) {
                    old_c.max_running.store(c.max_running, Ordering::Relaxed);
                    old_c.epoch.fetch_add(1, Ordering::Relaxed); // Інвалідуємо старі reconcile
                    states.push(old_c);
                } else {
                    states.push(Arc::new(ClusterState::new(c)));
                }
            }

            // Будимо всю чергу, щоб вони побачили нову місткість
            notify.notify_waiters();

            new_groups.push(Arc::new(GroupState {
                name: name.clone(),
                clusters: states,
                notify,
                queued,
            }));
        }
        self.groups.store(Arc::new(new_groups));
    }

    /// Bonus: correct a cluster's running counter against the engine's own count.
    /// `fetch` asks the engine how many queries it is running; it is slow.
    pub async fn reconcile<F, Fut>(&self, group: &str, cluster: &str, fetch: F)
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = u64>,
    {
        if let Some(state) = self.find(group, cluster) {
            // 1. Запам'ятовуємо стан і епоху ДО асинхронного запиту
            let baseline = state.running.load(Ordering::Relaxed);
            let start_epoch = state.epoch.load(Ordering::Relaxed);

            // 2. Довге очікування відповіді від рушія
            let target = fetch().await;

            // 3. Захист (Epoch Pattern): Якщо адмін робив reload або змінював ліміти,
            // поки ми чекали, дані від рушія вже нерелевантні. Скасовуємо.
            if state.epoch.load(Ordering::Relaxed) != start_epoch {
                return;
            }

            // 4. ФІКС INC-5: Компенсуюча транзакція (Drift correction)
            let _ = state
                .running
                .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
                    let drift = target as i64 - baseline as i64;
                    // .max(0) захищає від underflow, якщо завершилося більше запитів, ніж очікувалося
                    Some((current as i64 + drift).max(0) as u64)
                });
        }
    }
}

/// Proof of admission. The holder may run one query on `cluster()`.
pub struct Permit {
    state: Arc<ClusterState>,
    group_notify: Arc<Notify>,
    group_queued: Arc<AtomicU64>,
}

impl Drop for Permit {
    fn drop(&mut self) {
        // Звільняємо слот
        self.state.decrement();

        // Якщо в черзі є очікувачі, миттєво будимо одного з них
        if self.group_queued.load(Ordering::Relaxed) > 0 {
            self.group_notify.notify_one();
        }
    }
}

impl Permit {
    pub fn cluster(&self) -> &str {
        &self.state.name
    }

    pub fn release(self) {
        drop(self);
    }
}

#[derive(Debug)]
struct GroupState {
    name: String,
    clusters: Vec<Arc<ClusterState>>,
    notify: Arc<Notify>,
    queued: Arc<AtomicU64>,
}
