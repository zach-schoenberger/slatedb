// our microbenchmarks use pprof, but it doesn't work on windows
#![cfg(not(windows))]

//! Point lookups with and without pinned SST filters and indexes (RFC 0037).
//!
//! The layout mirrors a read-heavy service: a sorted run of several SSTs
//! under L0 SSTs whose key ranges span the whole keyspace, so only filters
//! can rule an L0 SST out. A `DbReader` with the default block cache serves
//! the lookups after a warm-up pass, so every unpinned filter and index read
//! is a cache hit and the benchmark measures CPU, not I/O.
//!
//! Lookups run at several concurrency levels on a multi-threaded runtime, to
//! model a service under high get volume: `1` is single-request latency,
//! `cores` saturates every worker, and `4 x cores` adds queueing on top. Each
//! sample times a batch of gets by wall clock, so criterion reports
//! throughput (gets/s) and the time per get is amortized across workers.
//! Concurrency exposes contention on shared state (block cache shards,
//! refcounts, the reader's state) that a sequential loop hides.
//!
//! - `absent`: keys inside every SST's range that no SST holds. Every
//!   candidate SST is a filter negative.
//! - `present_in_sorted_run`: keys that only the sorted run holds. Every L0
//!   SST is a filter negative, and the run's SST is read.
//!
//! Run: cargo bench -p slatedb --bench pinned_metadata_get

use std::sync::Arc;
use std::time::Duration;

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use object_store::memory::InMemory;
use object_store::ObjectStore;
use pprof::criterion::{Output, PProfProfiler};
use slatedb::config::{
    CompactionWorkerOptions, CompactorOptions, DbReaderOptions, FlushOptions, FlushType, Settings,
    SizeTieredCompactionSchedulerOptions,
};
use slatedb::size_tiered_compaction::SizeTieredCompactionSchedulerSupplier;
use slatedb::{CompactorBuilder, Db, DbReader, MetadataPinning};
use tokio::runtime::Builder;

const PATH: &str = "/bench/pinned_metadata_get";
const RUN_KEYS: usize = 50_000;
const L0_SSTS: usize = 8;
const KEYS_PER_L0: usize = 1_000;
const VALUE: [u8; 64] = [b'v'; 64];
/// Lookups rotate through this many keys.
const LOOKUP_KEYS: usize = 1_024;

type KeyFn = fn(usize) -> Vec<u8>;

fn run_key(i: usize) -> Vec<u8> {
    format!("key{:08}", i).into_bytes()
}

/// L0 keys interleave with the run's keys, so no L0 SST holds a run key.
fn l0_key(sst: usize, i: usize) -> Vec<u8> {
    format!("key{:08}-l0-{sst}", i * (RUN_KEYS / KEYS_PER_L0)).into_bytes()
}

fn absent_key(i: usize) -> Vec<u8> {
    format!("key{:08}-absent", i * (RUN_KEYS / LOOKUP_KEYS)).into_bytes()
}

fn present_key(i: usize) -> Vec<u8> {
    run_key(i * (RUN_KEYS / LOOKUP_KEYS))
}

async fn flush_memtable(db: &Db) {
    db.flush_with_options(FlushOptions {
        flush_type: FlushType::MemTable,
    })
    .await
    .expect("flush failed");
}

// A wall-clock wait for the compactor; benchmarks do not run under DST.
#[allow(clippy::disallowed_methods)]
async fn wait_for_empty_l0(db: &Db) {
    tokio::time::timeout(Duration::from_secs(120), async {
        while !db.manifest().l0().is_empty() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("compactor did not drain L0");
}

/// Writes a sorted run of several SSTs, then [`L0_SSTS`] L0 SSTs on top.
async fn populate(store: Arc<dyn ObjectStore>) {
    let compactor_options = CompactorOptions {
        poll_interval: Duration::from_millis(10),
        commit_compacted_interval: Duration::from_millis(10),
        scheduler_options: SizeTieredCompactionSchedulerOptions {
            min_compaction_sources: 1,
            ..Default::default()
        }
        .into(),
        worker: Some(CompactionWorkerOptions {
            compactions_poll_interval: Duration::from_millis(10),
            max_sst_size: 512 * 1024,
            ..Default::default()
        }),
        ..Default::default()
    };
    let db = Db::builder(PATH, store.clone())
        .with_settings(Settings {
            min_filter_keys: 0,
            l0_max_ssts: 64,
            l0_max_ssts_per_key: 64,
            ..Settings::default()
        })
        .with_compactor_builder(
            CompactorBuilder::new(PATH, store.clone())
                .with_scheduler_supplier(Arc::new(SizeTieredCompactionSchedulerSupplier::new()))
                .with_options(compactor_options),
        )
        .build()
        .await
        .expect("failed to open writer");
    for i in 0..RUN_KEYS {
        db.put(run_key(i), VALUE).await.expect("put failed");
    }
    flush_memtable(&db).await;
    wait_for_empty_l0(&db).await;
    db.close().await.expect("failed to close writer");

    // Reopen without a compactor so the L0 SSTs stay in L0.
    let db = Db::builder(PATH, store)
        .with_settings(Settings {
            min_filter_keys: 0,
            l0_max_ssts: 64,
            l0_max_ssts_per_key: 64,
            compactor_options: None,
            ..Settings::default()
        })
        .build()
        .await
        .expect("failed to reopen writer");
    for sst in 0..L0_SSTS {
        for i in 0..KEYS_PER_L0 {
            db.put(l0_key(sst, i), VALUE).await.expect("put failed");
        }
        flush_memtable(&db).await;
    }
    let manifest = db.manifest();
    assert_eq!(manifest.l0().len(), L0_SSTS);
    assert!(manifest
        .compacted()
        .iter()
        .any(|run| run.sst_views().len() > 1));
    db.close().await.expect("failed to close writer");
}

async fn open_reader(store: Arc<dyn ObjectStore>, mode: MetadataPinning) -> DbReader {
    let reader = DbReader::builder(PATH, store)
        .with_options(DbReaderOptions {
            metadata_pinning: mode,
            ..DbReaderOptions::default()
        })
        .build()
        .await
        .expect("failed to open reader");
    // Fill the pins and warm the block cache.
    for i in 0..LOOKUP_KEYS {
        reader.get(absent_key(i)).await.expect("get failed");
        reader.get(present_key(i)).await.expect("get failed");
    }
    reader
}

/// Issues `gets` lookups split across `concurrency` tasks and returns the
/// wall-clock time for the whole batch.
async fn concurrent_gets(
    reader: Arc<DbReader>,
    keys: Arc<Vec<Vec<u8>>>,
    present: bool,
    concurrency: usize,
    gets: u64,
) -> Duration {
    let concurrency_u64 = concurrency as u64;
    #[allow(clippy::disallowed_types)]
    let start = std::time::Instant::now();
    let tasks: Vec<_> = (0..concurrency)
        .map(|task| {
            let reader = reader.clone();
            let keys = keys.clone();
            let task_gets =
                gets / concurrency_u64 + u64::from((task as u64) < gets % concurrency_u64);
            // Spread starting points so tasks do not walk the keys in lockstep.
            let offset = task * LOOKUP_KEYS / concurrency;
            tokio::spawn(async move {
                for n in 0..task_gets as usize {
                    let key = &keys[(offset + n) % LOOKUP_KEYS];
                    let found = reader.get(key).await.expect("get failed");
                    assert_eq!(found.is_some(), present);
                }
            })
        })
        .collect();
    for task in tasks {
        task.await.expect("get task panicked");
    }
    start.elapsed()
}

fn bench_pinned_gets(c: &mut Criterion) {
    let cores = std::thread::available_parallelism()
        .expect("failed to read available parallelism")
        .get();
    let runtime = Builder::new_multi_thread()
        .worker_threads(cores)
        .enable_all()
        .build()
        .expect("failed to create runtime");
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    runtime.block_on(populate(store.clone()));

    let concurrency_levels = [1, cores, 4 * cores];

    let modes = [
        MetadataPinning::None,
        MetadataPinning::L0,
        MetadataPinning::All,
    ];
    let lookups: [(&str, KeyFn, bool); 2] = [
        ("absent", absent_key, false),
        ("present_in_sorted_run", present_key, true),
    ];
    for (name, key, present) in lookups {
        let keys: Arc<Vec<Vec<u8>>> = Arc::new((0..LOOKUP_KEYS).map(key).collect());
        let mut group = c.benchmark_group(format!("pinned_metadata_get/{name}"));
        group.throughput(Throughput::Elements(1));
        for mode in modes {
            let reader = Arc::new(runtime.block_on(open_reader(store.clone(), mode)));
            for concurrency in concurrency_levels {
                group.bench_with_input(
                    BenchmarkId::new(format!("{mode:?}"), concurrency),
                    &concurrency,
                    |b, &concurrency| {
                        b.to_async(&runtime).iter_custom(|gets| {
                            concurrent_gets(
                                reader.clone(),
                                keys.clone(),
                                present,
                                concurrency,
                                gets,
                            )
                        })
                    },
                );
            }
            runtime
                .block_on(reader.close())
                .expect("failed to close reader");
        }
        group.finish();
    }
}

criterion_group! {
    name = benches;
    config = Criterion::default()
        // This only runs when `--profile-time <num_seconds>` is set
        .with_profiler(PProfProfiler::new(100, Output::Protobuf));
    targets = bench_pinned_gets
}

criterion_main!(benches);
