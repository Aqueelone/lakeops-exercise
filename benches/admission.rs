use std::sync::{Arc, Barrier};
use std::thread;
use std::time::{Duration, Instant};

use admission::{ClusterConfig, ClusterPool, PoolConfig};
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};

fn pool() -> Arc<ClusterPool> {
    let clusters = (0..8)
        .map(|i| ClusterConfig::new(format!("trino-{i}"), 1_000_000))
        .collect();
    let mut config = PoolConfig::default().group("analytics", clusters);
    for i in 0..20 {
        config = config.group(
            format!("tenant-{i}"),
            vec![ClusterConfig::new(format!("t{i}"), 1_000_000)],
        );
    }
    ClusterPool::new(config)
}

/// One acquire + release, i.e. the admission overhead added to every query.
fn bench(c: &mut Criterion) {
    let mut group = c.benchmark_group("acquire_release");
    for threads in [1usize, 4, 16] {
        group.throughput(Throughput::Elements(threads as u64));
        group.bench_with_input(
            BenchmarkId::from_parameter(threads),
            &threads,
            |b, &threads| {
                let pool = pool();
                b.iter_custom(|iters| {
                    let barrier = Barrier::new(threads + 1);
                    thread::scope(|s| {
                        let workers: Vec<_> = (0..threads)
                            .map(|_| {
                                s.spawn(|| {
                                    barrier.wait();
                                    for _ in 0..iters {
                                        if let Ok(permit) = pool.try_acquire("analytics") {
                                            permit.release();
                                        }
                                    }
                                })
                            })
                            .collect();
                        barrier.wait();
                        let start = Instant::now();
                        for w in workers {
                            w.join().unwrap();
                        }
                        start.elapsed()
                    })
                });
            },
        );
    }
    group.finish();
}

criterion_group! {
    name = benches;
    config = Criterion::default().measurement_time(Duration::from_secs(3));
    targets = bench
}
criterion_main!(benches);
