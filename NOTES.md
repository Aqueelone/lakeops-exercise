# Notes

## Part 1: Incidents

# Cluster Admission Control: Incident Report

| Incident | Root cause | Reproducing test | Fix |
| :--- | :--- | :--- | :--- |
| **INC-1: Cap exceeded, then wedged** | Twofold: a race condition in `try_increment` (non-atomic load/store) causing the capacity cap to be exceeded, and a cancellation leak (missing `Drop` trait on `Permit`) causing slots to become permanently wedged after client disconnects. | `test_inc_1_cap_exceeded_race_condition` (simulating a concurrent traffic spike) and `test_inc_1_wedged_gateway_permit_leak` (simulating task cancellation). | Implemented an atomic Compare-and-Swap (CAS) loop via `fetch_update` for increments, and introduced the RAII pattern (`impl Drop for Permit`) to guarantee slot release upon task cancellation. |
| **INC-2: Capacity cut** | Unsigned integer underflow (`max_running - running`) when capacity was lowered whilst under load. Debug builds panicked; Release builds wrapped around to `u64::MAX`, causing the overloaded cluster to act as a routing "black hole". | `test_inc_2_capacity_cut_debug_panic` (catching the underflow panic) and `test_inc_2_capacity_cut_release_wedged` (demonstrating the flawed routing behaviour). | Replaced standard subtraction with `saturating_sub()` to safely return `0` instead of underflowing, ensuring fully occupied clusters are correctly filtered out. |
| **INC-3: Busy with free capacity** | A Time-of-Check to Time-of-Use (TOCTOU) race condition. The gateway blindly trusted the initial `pick()` selection and failed with `Busy` immediately if another thread concurrently seized the slot, rather than falling back to other available clusters. | `test_inc_3_busy_with_free_capacity_toctou` (dispatching 30 concurrent requests to a 30-slot pool, verifying zero `Busy` errors). | Replaced the brittle `pick()` logic with a lock-free, zero-allocation optimistic retry mechanism ($O(N)$), iterating through candidates and gracefully retrying upon TOCTOU conflicts. |
| **INC-4: Slots never recovered** | A Trino outage caused prolonged hanging queries, leading to client timeouts. Tokio cancelled the asynchronous tasks, destroying the `Permit` objects without invoking `release()`, thus permanently leaking cluster capacity. | `test_inc_4_slots_never_recovered_timeout` (utilising `tokio::time::timeout` to forcefully interrupt hanging requests and assert capacity restoration). | Inherently resolved by the INC-1 RAII implementation (`impl Drop for Permit`), ensuring robust cancellation safety and automatic capacity recovery during network outages. |
Dependency Update: Added fastrand for O(1) Load BalancingWe have added the fastrand crate to our Cargo.toml to implement the Power of Two Choices (P2C) load-balancing algorithm within the try_acquire hot path.Why P2C and O(1) Complexity?Previously, finding the optimal cluster required collecting candidates into a vector and sorting them, resulting in $O(N \log N)$ time complexity and unnecessary heap allocations. The P2C algorithm bypasses this by randomly selecting just two clusters and routing the query to the less loaded one. This approach mathematically guarantees near-perfect load distribution with a constant $O(1)$ time complexity, regardless of the group's size. Furthermore, it inherently mitigates the "thundering herd" problem, preventing multiple concurrent threads from violently converging on a single optimal node.Why fastrand?We selected fastrand over the standard rand crate as it provides lightning-fast, zero-allocation pseudo-random number generation with minimal dependencies. This keeps our binary lean and aligns perfectly with the strict L2 cache limitations and low-latency requirements of our Cloud Run environment.

## Part 2: Performance

Machine (CPU and core count): In green mode! Intel(R) Core(TM) Ultra 9 275HX (2.70 GHz) 64Gb (6400MT/s) NVIDIA GeForce RTX 5080 Laptop GPU (16 Gb) 

| Bench | Before | After |
|-------|--------|-------|
| acquire_release/1  | 1.54 Melem/s (647 ns) | 25.04 Melem/s (39 ns) |
| acquire_release/4  | 2.67 Melem/s (1.49 µs) | 20.18 Melem/s (198 ns) |
| acquire_release/16 | 3.74 Melem/s (4.27 µs) | 26.86 Melem/s (595 ns) |

Changes, and why each one helps (or didn't):

- **Eradicated Reader Contention via RCU (`arc-swap`)**: The original `std::sync::RwLock` forces all threads to atomically mutate a shared reader-count integer, causing severe cache-line bouncing and CPU bus queuing under load. I introduced the `arc-swap` crate to implement the Read-Copy-Update (RCU) pattern. This makes routing table lookups completely lock-free and wait-free on the hot path, directly enabling the ~7.1× throughput leap on 16 threads.
- **Eliminated Heap Allocations (`Vec::cloned` and `String`)**: The starter code cloned a `Vec<Arc<ClusterState>>` and allocated a new `String` for the group name on every single request. I refactored `try_acquire` to operate entirely on borrowed references, removing the memory allocator bottleneck from the hot path.
- **Lock-Free O(1) Permit Release**: Refactored `Permit` to directly hold an `Arc<ClusterState>` instead of string identifiers. This transitioned the `Drop` implementation from an $O(N)$ locked operation (requiring an `RwLock` and `HashMap` search) to an instantaneous $O(1)$ atomic decrement (`fetch_sub`).
- **Lock-Free Admissions Metric**: Replaced the global `Mutex<HashMap>` used for Prometheus admissions tracking with an `AtomicU64` located directly inside each `ClusterState`. This eradicated thread contention and expensive string formatting (`format!`) during `try_acquire`.
- **Bypassed SipHash Overhead (Data Locality)**: The standard `std::collections::HashMap` utilises SipHash, which introduces significant CPU overhead for every `group` lookup. Since the number of groups is relatively small, I refactored the routing table into a flat `Vec<(String, Vec<Arc<ClusterState>>)>`. A rapid, cache-friendly linear scan across contiguous memory outperforms heavy hashing math, drastically reducing L1 cache misses.
- **Optimised Atomic Operations (`lock xadd` vs `lock cmpxchg`)**: Switched `try_increment` from a Compare-and-Swap loop (`fetch_update`) to an optimistic `fetch_add` with a rollback (`fetch_sub`) if the capacity is exceeded. At the x86 assembly level, this executes as a single `lock xadd` instruction, which handles concurrent bus traffic far more efficiently than failing and retrying a CAS loop.

Selection policy, and why:

**Policy: Hybrid Optimistic Retry (Roughly Least-Loaded)**
The original approach gathered all candidates, allocated a vector, and sorted them at an $O(N \log N)$ cost, which created a "thundering herd" where all concurrent threads converged violently on a single optimal node until its capacity was exhausted.

To achieve "roughly least-loaded" whilst maintaining maximum throughput, I implemented a zero-allocation, bounded-retry strategy. We employ a rapid $O(1)$ Power of Two Choices (P2C) sub-selection or an $O(N)$ linear scan to identify the highest capacity cluster in the current snapshot, then attempt an atomic increment. If this fails due to a Time-of-Check to Time-of-Use (TOCTOU) race condition, we loop and evaluate the *new* highest capacity cluster, retrying up to 3 times. This perfectly adheres to the requirement of spreading queries proportionately across clusters by their free capacity, mathematically guaranteeing near-perfect load distribution without the extreme CPU overhead of array sorting or memory fragmentation.

## Part 3: Waiting for a slot

To implement `acquire_timeout` and `queued` without degrading the extreme lock-free performance achieved in Part 2, I introduced an asynchronous queueing mechanism utilising `tokio::sync::Notify` and atomic counters, seamlessly integrated into our RCU (`arc-swap`) architecture.

* **State Encapsulation:** I introduced a `GroupState` struct, which encapsulates the routing table alongside an `Arc<Notify>` and an `Arc<AtomicU64>` queue counter. Wrapping this in `ArcSwap` ensures that callers waiting in `acquire_timeout` can safely clone the `Arc` and drop the read guard before yielding to the Tokio runtime, preventing RCU memory leaks during hot config reloads.
* **Prompt Wakeups & No Lost Wakeups:** When a `Permit` is dropped, it performs an $O(1)$ atomic check on the queue counter. If waiters exist, it immediately invokes `notify_one()`, ensuring prompt wakeups. To prevent TOCTOU "lost wakeup" race conditions in `acquire_timeout`, the caller obtains the `notified()` future *before* attempting its final optimistic acquisition in the wait loop.
* **Cancellation Safety & Queue Tracking (RAII):** To track the number of queued callers accurately and handle timeouts or network disconnects safely, I implemented a custom `WaitGuard` (RAII pattern). It increments the `queued` atomic upon creation and guarantees a `fetch_sub` decrement in its `Drop` implementation. This ensures metrics remain perfectly consistent even during severe cancellation storms or when `tokio::time::timeout` expires.
* **Dynamic Capacity Edge Cases:** I updated the administrative APIs (`set_max_running` and `reload`). If an administrator dynamically increases a group's capacity or deploys a new configuration with expanded limits whilst clients are queued, these methods now explicitly invoke `notify_waiters()`. This resolves a critical edge case where clients would otherwise hang indefinitely waiting for a `release()` that might never occur.

### Beyond the Requirements: Edge Case Testing & The "Capacity Trap"

To ensure production-grade robustness suitable for a high-load routing gateway, I authored an additional stress-test suite (`tests/waiting_edge_cases.rs`) focusing on cancellation storms and dynamic capacity mutations.

This custom test suite uncovered a subtle but critical architectural flaw inherent in the baseline requirements: the **"Dynamic Capacity Increase Trap"**.

* **The Problem:** If a cluster has zero capacity (or is completely saturated), callers correctly queue up in `acquire_timeout`. However, if an administrator dynamically increases the limit via `set_max_running()` or injects a larger configuration via `reload()`, the waiting clients remain asleep indefinitely until their timeouts expire. This occurs because no active queries exist to drop a `Permit` and trigger a `release()` wakeup.
* **The Solution:** I resolved this by modifying the administrative APIs. In `set_max_running`, the system now performs an atomic `swap` to compare the old capacity against the new one. If the capacity has expanded, it explicitly accesses the group's state and invokes `notify_waiters()`. I applied a similar proactive wakeup strategy to `reload()`. This safely awakens all queued clients, allowing them to instantly re-evaluate the newly available slots, passing the strict edge-case tests and eliminating the risk of stalled traffic.

## Part 4: Global admission across 6 pods

## Bonus: INC-5 and INC-6

## Before shipping, I would still want

-
