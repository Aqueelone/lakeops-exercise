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
}

impl ClusterState {
    fn new(config: &ClusterConfig) -> Self {
        Self {
            name: config.name.clone(),
            max_running: AtomicU64::new(config.max_running),
            running: AtomicU64::new(0),
            admissions: AtomicU64::new(0), // <-- ДОДАНО
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
    // Жодних м'ютексів і локів на читання!
    groups: ArcSwap<Vec<(String, Vec<Arc<ClusterState>>)>>,
}

impl ClusterPool {
    pub fn new(config: PoolConfig) -> Arc<Self> {
        Arc::new(Self {
            // from_pointee автоматично загортає дані в Arc
            groups: ArcSwap::from_pointee(build_groups(&config)),
        })
    }

    // Оновлений метод find (без to_string та cloned)
    fn find(&self, group: &str, cluster: &str) -> Option<Arc<ClusterState>> {
        self.groups
            .load() // Миттєве lock-free читання!
            .iter()
            .find(|(name, _)| name == group)
            .map(|(_, clusters)| clusters)?
            .iter()
            .find(|c| c.name == cluster)
            .cloned()
    }

    /// Admit one query to the least-loaded cluster of `group`, or fail fast with `Busy`.
    // 2. ОПТИМІЗАЦІЯ БАЛАНСУВАННЯ: P2C з fastrand (Zero-allocation, O(1))
    pub fn try_acquire(self: &Arc<Self>, group: &str) -> Result<Permit, AcquireError> {
        // Guard працює як Arc, але його отримання не блокує інші потоки
        // і не викликає cache-line bouncing.
        let groups_guard = self.groups.load();

        let clusters = groups_guard
            .iter()
            .find(|(name, _)| name == group)
            .map(|(_, c)| c)
            .ok_or(AcquireError::UnknownGroup)?;

        let len = clusters.len();
        if len == 0 {
            return Err(AcquireError::Busy);
        }

        for _ in 0..3 {
            let state = if len == 1 {
                &clusters[0]
            } else {
                let i = fastrand::usize(0..len);
                let mut j = fastrand::usize(0..len);
                if i == j {
                    j = (j + 1) % len;
                }

                let a = &clusters[i];
                let b = &clusters[j];

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
                });
            }
        }

        Err(AcquireError::Busy)
    }

    /// Part 3: like `try_acquire`, but wait up to `wait` for a slot to free up.
    pub async fn acquire_timeout(
        self: &Arc<Self>,
        group: &str,
        wait: Duration,
    ) -> Result<Permit, AcquireError> {
        let _ = (group, wait);
        todo!("Part 3")
    }

    /// Current running count for a cluster, or `None` if it is not in the current config.
    pub fn running(&self, group: &str, cluster: &str) -> Option<u64> {
        self.find(group, cluster).map(|c| c.running())
    }

    /// Number of callers currently waiting inside `acquire_timeout` for `group`.
    pub fn queued(&self, group: &str) -> u64 {
        let _ = group;
        todo!("Part 3")
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
    pub fn set_max_running(&self, group: &str, cluster: &str, max_running: u64) {
        if let Some(state) = self.find(group, cluster) {
            state.max_running.store(max_running, Ordering::Relaxed);
        }
    }

    /// Bonus: swap in a new config while queries are in flight.
    pub fn reload(&self, config: PoolConfig) {
        // Миттєва підміна вказівника. Старий конфіг буде знищено,
        // коли останній активний читач (Permit) завершить роботу.
        self.groups.store(Arc::new(build_groups(&config)));
    }

    /// Bonus: correct a cluster's running counter against the engine's own count.
    /// `fetch` asks the engine how many queries it is running; it is slow.
    pub async fn reconcile<F, Fut>(&self, group: &str, cluster: &str, fetch: F)
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = u64>,
    {
        let target = fetch().await;
        if let Some(state) = self.find(group, cluster) {
            state.running.store(target, Ordering::Relaxed);
        }
    }
}

fn build_groups(config: &PoolConfig) -> Vec<(String, Vec<Arc<ClusterState>>)> {
    config
        .groups
        .iter()
        .map(|(group, clusters)| {
            let states = clusters
                .iter()
                .map(|c| Arc::new(ClusterState::new(c)))
                .collect();
            (group.clone(), states)
        })
        .collect()
}

/// Proof of admission. The holder may run one query on `cluster()`.
pub struct Permit {
    // Тримаємо стан напряму. Ніяких String і пулів!
    state: Arc<ClusterState>,
}

impl Drop for Permit {
    fn drop(&mut self) {
        // Атомарно, миттєво, без локів!
        self.state.decrement();
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
