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
#[repr(align(64))] // OPTIMIZATION: Eliminating False Sharing Between Threads at the L1 Cache Level
struct ClusterState {
    name: String,
    max_running: AtomicU64,
    running: AtomicU64,
    admissions: AtomicU64, // <-- added
    epoch: AtomicU64,      // <-- added: Race condition safeguard during reloads
    // OPTIMIZATION: Keep the reference here to avoid cloning unnecessary Arcs in Permit.
    group_notify: Arc<Notify>,
    group_queued: Arc<AtomicU64>,
}

impl ClusterState {
    fn new(config: &ClusterConfig, group_notify: Arc<Notify>, group_queued: Arc<AtomicU64>) -> Self {
        Self {
            name: config.name.clone(),
            max_running: AtomicU64::new(config.max_running),
            running: AtomicU64::new(0),
            admissions: AtomicU64::new(0),
            epoch: AtomicU64::new(0),
            group_notify,
            group_queued,
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

    // 1. ATOMIC OPERATION OPTIMISATION (lock xadd instead of lock cmpxchg)
    fn try_increment(&self) -> bool {
        // Instantaneous increment without loops (the fastest atomic operation)
        let prev = self.running.fetch_add(1, Ordering::Relaxed);

        if prev >= self.max_running() {
            // Rollback if the capacity limit is exceeded
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
            // We create them here once for the group
            let notify = Arc::new(Notify::new());
            let queued = Arc::new(AtomicU64::new(0));

            let states = clusters
                .iter()
                // We pass the link to each cluster
                .map(|c| Arc::new(ClusterState::new(c, Arc::clone(&notify), Arc::clone(&queued))))
                .collect();

            initial.push(Arc::new(GroupState {
                name: name.clone(),
                clusters: states,
                notify,
                queued,
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

    // Extracting cluster selection logic (P2C) into a dedicated method
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
                    state: Arc::clone(state), // Just only ones Arc::clone!
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
        // Cloning `Arc<GroupState>` to avoid holding `ArcSwapGuard` across `.await` points (which would block config updates)
        let group_state = {
            let guard = self.groups.load();
            guard
                .iter()
                .find(|g| g.name == group)
                .cloned()
                .ok_or(AcquireError::UnknownGroup)?
        };

        // 1. Fast path: a slot might already be available
        if let Ok(permit) = self.acquire_from_group(&group_state) {
            return Ok(permit);
        }

        // 2. Enter the queue
        group_state.queued.fetch_add(1, Ordering::Relaxed);

        // RAII Guard: ensures the counter is decremented even if the Future is cancelled (cancellation safety)
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

        // 3. Wait with a timeout
        let wait_fut = tokio::time::timeout(wait, async {
            loop {
                // CRUCIAL: Acquire the `notified` future BEFORE checking for slot availability.
                // This entirely eliminates race conditions (lost wakeups).
                let notified = group_state.notify.notified();

                if let Ok(permit) = self.acquire_from_group(&group_state) {
                    return permit;
                }

                // Sleep, awaiting a notify_one() from another caller's Permit::drop
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
        // Locate the cluster using the existing find method
        // and read the atomic value without any locks
        self.find(group, cluster)
            .map(|c| c.admissions.load(Ordering::Relaxed))
            .unwrap_or(0)
    }

    /// Admin API: change a cluster's capacity at runtime.
    /// fixed after fail test: edge_case_dynamic_capacity_increase_wakes_waiters
    pub fn set_max_running(&self, group: &str, cluster: &str, max_running: u64) {
        if let Some(state) = self.find(group, cluster) {
            let old = state.max_running.swap(max_running, Ordering::Relaxed);

            // NEW: Invalidate background reconciliations since capacity has mutated
            state.epoch.fetch_add(1, Ordering::Relaxed);

            // If capacity has expanded, we must awaken those waiting in the queue!
            if max_running > old {
                let guard = self.groups.load();
                if let Some(g) = guard.iter().find(|g| g.name == group) {
                    // Utilising notify_waiters() instead of notify_one().
                    // After all, capacity might have increased by 10, and if we awaken only one,
                    // the other 9 slots would sit idle. Those who still lack a slot
                    // will simply re-attempt and go back to sleep.
                    g.notify.notify_waiters();
                }
            }
        }
    }

    /// Bonus: swap in a new config while queries are in flight.
    /// fixed after fail test: edge_case_dynamic_capacity_increase_wakes_waiters
    pub fn reload(&self, config: PoolConfig) {
        let old_guard = self.groups.load();

        // 1. Gather legacy groups for reuse
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
                // FIX INC-6: Reuse existing state!
                if let Some(old_c) = old_clusters.remove(&c.name) {
                    old_c.max_running.store(c.max_running, Ordering::Relaxed);
                    old_c.epoch.fetch_add(1, Ordering::Relaxed);
                    states.push(old_c);
                } else {
                    // If the cluster is new, we create it with existing notify/queued groups
                    states.push(Arc::new(ClusterState::new(c, Arc::clone(&notify), Arc::clone(&queued))));
                }
            }

            // Awaken the entire queue so they can observe the new capacity
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
            // 1. Snapshot the state and epoch BEFORE the asynchronous request
            let baseline = state.running.load(Ordering::Relaxed);
            let start_epoch = state.epoch.load(Ordering::Relaxed);

            // 2. Lengthy wait for the engine's response
            let target = fetch().await;

            // 3. Safeguard (Epoch Pattern): If an admin executed a reload or altered limits,
            // whilst we were waiting, the engine data is no longer relevant. Abort.
            if state.epoch.load(Ordering::Relaxed) != start_epoch {
                return;
            }

            // 4. FIX INC-5: Compensating transaction (Drift correction)
            let _ = state
                .running
                .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
                    let drift = target as i64 - baseline as i64;
                    // .max(0) safeguards against underflow if more queries completed than anticipated
                    Some((current as i64 + drift).max(0) as u64)
                });
        }
    }
}

/// Proof of admission. The holder may run one query on `cluster()`.
pub struct Permit {
    state: Arc<ClusterState>,
}

impl Drop for Permit {
    fn drop(&mut self) {
        // A slot releasing
        self.state.decrement();

        // If there are waiters in the queue, we immediately wake one of them up.
        // We take the reference directly from the cluster state itself!
        if self.state.group_queued.load(Ordering::Relaxed) > 0 {
            self.state.group_notify.notify_one();
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
